WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek
    FROM claims
    WHERE claimed_amount >= 5774.6
      AND status IN ('REJECTED', 'OPEN')
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        reported_at_dayofweek,
        status,
        ROUND(STDDEV(reported_at_dayofweek), 2) AS stddev_reported_at_dayofweek,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        MIN(reported_at) AS first_reported_at
    FROM valid
    GROUP BY DATE_TRUNC('MONTH', reported_at), reported_at_dayofweek, status
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        reported_at_month,
        reported_at_dayofweek,
        status,
        status_list,
        COUNT(*) OVER (PARTITION BY status, DATE_TRUNC('MONTH', first_reported_at) ORDER BY first_reported_at DESC, reported_at_month, reported_at_dayofweek, status ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER (PARTITION BY reported_at_dayofweek, status) AS rows_in_reported_at_dayofweek_status,
        ROUND(PERCENT_RANK() OVER w_status_first_reported_at_mon_2, 3) AS first_reported_at_percent_rank,
        SUM(stddev_reported_at_dayofweek) OVER (PARTITION BY YEAR(first_reported_at) ORDER BY stddev_reported_at_dayofweek DESC, reported_at_month, reported_at_dayofweek, status ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS stddev_reported_at_dayofweek_sum_7,
        ROUND(AVG(reported_at_dayofweek) OVER (PARTITION BY status ORDER BY first_reported_at DESC, reported_at_month, reported_at_dayofweek, status ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS reported_at_dayofweek_avg_4,
        MAX(reported_at_dayofweek) OVER (PARTITION BY reported_at_dayofweek, status) AS reported_at_dayofweek_status_max_reported_at_day
    FROM per_group
    WINDOW w_status_first_reported_at_mon_2 AS (PARTITION BY status, DATE_TRUNC('MONTH', first_reported_at) ORDER BY first_reported_at DESC)
),
scored AS (
    -- Label and score rows
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM ranked
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reported_at_dayofweek, reported_at_month, status, reported_at_dayofweek_status_max_reported_at_day
    FROM scored
) src
PIVOT (
    SUM(reported_at_dayofweek_status_max_reported_at_day) AS total, COUNT(reported_at_dayofweek_status_max_reported_at_day) AS n
    FOR status IN ('OPEN', 'APPROVED')
)
