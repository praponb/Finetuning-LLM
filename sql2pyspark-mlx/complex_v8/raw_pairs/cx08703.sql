WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        interests,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown,
        DATE_TRUNC('QUARTER', signup_date) AS signup_date_quarter,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown_2
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'indie')
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        username,
        plan,
        interests,
        plan_or_unknown,
        plan_or_unknown_2,
        CONCAT_WS('-', plan_or_unknown_2, plan_or_unknown) AS plan_or_unknown_2_plan_or_unknown_label,
        TO_DATE(signup_date_quarter) AS signup_date_quarter_date,
        SUBSTRING(TRIM(plan_or_unknown_2), 1, 2) AS plan_or_unknown_2_prefix
    FROM filtered
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(plan_or_unknown_2, 'UNKNOWN') AS plan_or_unknown_2_or_unknown,
        SUBSTRING(TRIM(username), 1, 4) AS username_prefix,
        SIZE(interests) AS interests_count
    FROM enriched
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_plan_or_unknown_by_signup_da, 3) AS signup_date_quarter_date_cume_dist,
        SUM(interests_count) OVER (PARTITION BY plan_or_unknown_2 ORDER BY signup_date_quarter_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_interests_count,
        COUNT(*) OVER (PARTITION BY plan_or_unknown_2_or_unknown, plan) AS rows_in_plan_or_unknown_2_or_unknown_plan,
        MIN(interests_count) OVER (PARTITION BY plan_or_unknown_2_or_unknown, plan) AS plan_or_unknown_2_or_unknown_plan_min_interests_,
        SIZE(COLLECT_SET(plan_or_unknown_2) OVER (PARTITION BY plan_or_unknown_2_or_unknown, plan)) AS distinct_plan_or_unknown_2_in_plan_or_unknown_2_
    FROM tagged
    WINDOW w_plan_or_unknown_by_signup_da AS (PARTITION BY plan_or_unknown ORDER BY signup_date_quarter_date DESC)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(plan_or_unknown_2_or_unknown, 'ALL') AS plan_or_unknown_2_or_unknown,
    COALESCE(plan_or_unknown_2, 'ALL') AS plan_or_unknown_2,
    distinct_plan_or_unknown_2_in_plan_or_unknown_2_,
    GROUPING_ID() AS gid,
    SUM(signup_date_quarter_date_cume_dist) FILTER (WHERE LENGTH(TRIM(username)) >= 5) AS signup_date_quarter_date_cume_dist_length_trim_u,
    COUNT(*) AS n_rows
FROM windowed
GROUP BY ROLLUP(plan_or_unknown_2_or_unknown, plan_or_unknown_2, distinct_plan_or_unknown_2_in_plan_or_unknown_2_)
