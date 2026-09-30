WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
),
holder_id_stats AS (
    -- Avg premium_bucket per holder_id
    SELECT holder_id, AVG(premium_bucket) AS avg_premium_bucket
    FROM prepared
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_bucket,
        ROUND(s.premium_bucket - g.avg_premium_bucket, 2) AS premium_bucket_minus_avg
    FROM prepared s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium_bucket > g.avg_premium_bucket
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        premium_bucket,
        status_len,
        avg_premium_bucket,
        premium_bucket_minus_avg,
        ROUND(premium_bucket_minus_avg / NULLIF(SUM(premium_bucket_minus_avg) OVER (PARTITION BY MONTH(end_date)), 0), 4) AS premium_bucket_minus_avg_share_of_end_date_month,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status_len ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY status_len ORDER BY premium DESC, id), 3) AS premium_cume_dist,
        FIRST_VALUE(premium) OVER (PARTITION BY status_len ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        ROUND(avg_premium_bucket - AVG(avg_premium_bucket) OVER (PARTITION BY MONTH(end_date)), 2) AS avg_premium_bucket_vs_end_date_month_avg
    FROM compared
)
SELECT
    id,
    premium_bucket,
    status_len,
    premium_bucket_minus_avg,
    premium_cume_dist,
    first_premium,
    avg_premium_bucket_vs_end_date_month_avg,
    ROUND(first_known_premium / NULLIF(avg_premium_bucket, 0), 3) AS first_known_premium_per_avg_premium_bucket
FROM ranked
WHERE premium_bucket < 4280
ORDER BY id
