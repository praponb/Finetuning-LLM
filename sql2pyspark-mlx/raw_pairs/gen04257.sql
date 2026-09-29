WITH enriched AS (
    SELECT
        e.id,
        e.order_id,
        e.carrier,
        e.shipped_date,
        e.delivered_date,
        e.cost,
        e.status,
        b.channel,
        b.order_date
    FROM shipments e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
enriched_shipments AS (
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.delivered_date,
        e.cost,
        e.channel,
        b.discount
    FROM enriched e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
metrics AS (
    SELECT
        id,
        shipped_date,
        delivered_date,
        channel,
        discount,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY cost, id) AS cost_bucket,
        AVG(discount) OVER (PARTITION BY order_id) AS order_id_avg_discount
    FROM enriched_shipments
),
ranked AS (
    SELECT
        id,
        shipped_date,
        cost_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    SELECT
        *,
        CASE WHEN cost_bucket >= 7 THEN 'High' WHEN cost_bucket >= 4 THEN 'Medium' ELSE 'Low' END AS cost_bucket_band,
        DATE_TRUNC('week', shipped_date) AS shipped_date_period
    FROM ranked
    WHERE shipped_date > '2024-09-01'
),
metrics_shipments AS (
    SELECT
        id,
        shipped_date,
        AVG(cost_bucket) OVER (ORDER BY CAST(shipped_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_bucket_avg_30d,
        MAX(cost_bucket) OVER (ORDER BY CAST(shipped_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cost_bucket_max_7d,
        AVG(cost_bucket) OVER (ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_bucket_avg_3
    FROM cleaned
)
SELECT
    CASE WHEN cost_bucket_max_7d >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_bucket_max_7d_tier,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    PERCENTILE_APPROX(cost_bucket_avg_3, ARRAY(0.25, 0.5, 0.75)) AS cost_bucket_avg_3_quartiles,
    PERCENTILE_APPROX(cost_bucket_avg_3, 0.5) AS cost_bucket_avg_3_p50
FROM metrics_shipments
GROUP BY GROUPING SETS ((CASE WHEN cost_bucket_max_7d >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', shipped_date), DATE_TRUNC('YEAR', shipped_date)), (CASE WHEN cost_bucket_max_7d >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', shipped_date)), ());
