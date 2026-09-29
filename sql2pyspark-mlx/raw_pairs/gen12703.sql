WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS is_active,
        TRIM(status) AS clean_status
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND end_date > TO_DATE('2023-10-09')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.monthly_fee,
        e.status,
        e.is_active,
        c.name,
        c.country
    FROM valid e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(is_active) OVER (PARTITION BY status) AS status_max_is_active,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY is_active DESC, id) AS is_active_bucket,
        STDDEV_SAMP(is_active) OVER (PARTITION BY country) AS country_stddev_samp_is_active,
        RANK() OVER (PARTITION BY customer_id, country ORDER BY is_active DESC) AS is_active_rank
    FROM with_dims
),
with_windows_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY country, status ORDER BY country_stddev_samp_is_active, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        LAG(country_stddev_samp_is_active, 1) OVER (PARTITION BY country ORDER BY monthly_fee, id) AS prev_country_stddev_samp_is_active,
        FIRST_VALUE(is_active_rank) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY country_stddev_samp_is_active, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_active_rank,
        NTILE(4) OVER (PARTITION BY status ORDER BY is_active DESC, id) AS is_active_bucket_2,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY is_active_rank, id) AS is_active_rank_bucket
    FROM with_windows
    WHERE monthly_fee >= 70
)
-- Join a dimension and aggregate
SELECT
    c.segment,
    s.status,
    COUNT(*) AS rows_total,
    ROUND(SUM(s.is_active), 2) AS total_is_active,
    GROUPING_ID() AS level_id
FROM with_windows_subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY CUBE (c.segment, s.status)
ORDER BY total_is_active NULLS FIRST;
