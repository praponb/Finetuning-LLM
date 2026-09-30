WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER w_part cost_bucket,
        NTILE(3) OVER w_part cost_bucket_2
    FROM shipments
    WINDOW w_part AS (ORDER BY cost, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_bucket NULLS LAST, id) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(cost_bucket_2) OVER (PARTITION BY status) status_stddev_samp_cost_bucket_2,
        NTILE(4) OVER (PARTITION BY carrier ORDER BY cost DESC, id) cost_bucket_3,
        MIN(cost_bucket) OVER (PARTITION BY status, carrier) status_min_cost_bucket,
        SUM(cost_bucket) OVER (PARTITION BY carrier, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost_bucket,
        SUM(cost_bucket_2) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost_bucket_2
    FROM deduped
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    carrier,
    status,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    APPROX_PERCENTILE(status_stddev_samp_cost_bucket_2, 0.95) AS status_stddev_samp_cost_bucket_2_p95,
    RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', delivered_date) ORDER BY APPROX_PERCENTILE(status_stddev_samp_cost_bucket_2, 0.95) DESC) AS status_stddev_samp_cost_bucket_2_p95_rank
FROM metrics_shipments
GROUP BY 1, 2, 3
ORDER BY carrier;
