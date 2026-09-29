WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CASE WHEN method IN ('card', 'bank_transfer') THEN 'Tier 1' ELSE 'Other' END AS method_group,
        DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-04-23 13:00:00')
      AND method IS DISTINCT FROM 'cash'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        paid_at,
        method_group,
        days_since_paid_at,
        SUM(days_since_paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_days_since_paid_at,
        MIN(days_since_paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_days_since_paid_at,
        AVG(days_since_paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_days_since_paid_at
    FROM cleaned
    WHERE days_since_paid_at BETWEEN 41 AND 77
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        days_since_paid_at,
        running_sum_days_since_paid_at,
        running_avg_days_since_paid_at
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY days_since_paid_at DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.days_since_paid_at,
        d.amount,
        d.order_date
    FROM deduped a
    JOIN orders d
        ON a.order_id = d.id
)
-- Grouped metrics
SELECT
    order_id,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    APPROX_PERCENTILE(days_since_paid_at, ARRAY(0.25, 0.5, 0.75)) AS days_since_paid_at_quartiles,
    STDDEV_SAMP(amount) AS amount_stddev
FROM joined
GROUP BY order_id
ORDER BY amount_stddev ASC NULLS LAST
