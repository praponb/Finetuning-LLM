WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(shipped_date, delivered_date) / 7 AS weeks_delivered_date_to_shipped_date,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM shipments
    WHERE order_id IS NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost NULLS FIRST, id) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.order_date,
        p.channel,
        p.status AS order_status
    FROM ranked src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
    WHERE p.shipping_fee BETWEEN 15 AND 30
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        status,
        resolved_carrier,
        channel,
        order_status,
        LEAD(weeks_delivered_date_to_shipped_date, 1) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_weeks_delivered_date_to_shipped_date,
        DENSE_RANK() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_dense_rank
    FROM with_dims
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(cost_dense_rank / SUM(cost_dense_rank) OVER (PARTITION BY channel), 4) AS cost_dense_rank_share,
        SUM(next_weeks_delivered_date_to_shipped_date) OVER (PARTITION BY YEAR(delivered_date), status) AS delivered_date_year_sum_next_weeks_delivered_date_to_shipped_date
    FROM enriched
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', delivered_date) AS year_start,
        DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
        COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count,
        COUNT(*) AS row_count,
        COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count
    FROM enriched_shipments
    GROUP BY year_start, quarter_start
)
SELECT
    year_start,
    quarter_start,
    delivered_count,
    row_count,
    web_count
FROM stats
ORDER BY quarter_start DESC, year_start DESC;
