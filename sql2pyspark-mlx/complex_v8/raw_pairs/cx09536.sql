WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status
    FROM policies
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        premium,
        end_date,
        status,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status)) AS distinct_holder_id_in_status,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        ROUND(AVG(premium) OVER (PARTITION BY status), 2) AS status_avg_premium,
        DENSE_RANK() OVER (ORDER BY premium ASC) AS premium_dense_rank
    FROM cleaned_policys
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM windowed
    WHERE premium_vs_status_avg > (SELECT MIN(premium_vs_status_avg) FROM windowed)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        premium,
        end_date,
        status,
        premium_vs_status_avg,
        distinct_holder_id_in_status,
        premium_share_of_status,
        status_avg_premium
    FROM with_activity
    WHERE premium_dense_rank <= 5
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, status, premium_vs_status_avg
    FROM top_rows
) src
PIVOT (
    COUNT(premium_vs_status_avg)
    FOR status IN ('CANCELLED', 'ACTIVE', 'LAPSED', 'EXPIRED')
)
ORDER BY end_date
