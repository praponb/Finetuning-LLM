WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        CEIL(cost) AS cost_whole,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE shipped_date > DATE '2023-07-02'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.shipping_fee,
        o.amount
    FROM cleaned s
    LEFT JOIN orders o
        ON s.order_id <=> o.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY carrier ORDER BY shipped_date) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost_whole,
        carrier_clean,
        amount
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipping_fee DESC, id) = 1
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        cost_whole,
        carrier_clean,
        DENSE_RANK() OVER (PARTITION BY MONTH(shipped_date) ORDER BY cost_whole DESC NULLS LAST) AS cost_whole_dense_rank,
        AVG(amount) OVER (PARTITION BY DATE_TRUNC('month', delivered_date)) AS delivered_date_period_avg_amount,
        SUM(amount) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        PERCENT_RANK() OVER w_grp AS cost_whole_percent_rank,
        SUM(amount) OVER (ORDER BY cost_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM deduped
    WINDOW w_grp AS (ORDER BY cost_whole DESC NULLS FIRST)
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', shipped_date) AS activity_month,
        CASE WHEN cumulative_amount >= 8776 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_amount_tier,
        COUNT(*) AS event_count,
        ROUND(AVG(cost_whole_dense_rank), 2) AS avg_cost_whole_dense_rank,
        ROUND(AVG(cost_whole_percent_rank), 2) AS avg_cost_whole_percent_rank,
        APPROX_PERCENTILE(delivered_date_period_avg_amount, 0.9) AS delivered_date_period_avg_amount_p90,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(cost_whole_percent_rank) AS max_cost_whole_percent_rank
    FROM enriched_shipments
    GROUP BY DATE_TRUNC('MONTH', shipped_date), CASE WHEN cumulative_amount >= 8776 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    activity_month,
    cumulative_amount_tier,
    event_count,
    delivered_date_period_avg_amount_p90,
    max_cost_whole_percent_rank
FROM grouped
ORDER BY activity_month NULLS FIRST, delivered_date_period_avg_amount_p90;
