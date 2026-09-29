WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown,
        LENGTH(TRIM(country)) AS country_len
    FROM listeners
    WHERE UPPER(TRIM(username)) LIKE 'H%'
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.is_skipped = TRUE
              AND p.ms_played > 160264
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        plan,
        country,
        interests,
        plan_or_unknown,
        country_len,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown_2
    FROM with_activity
    WHERE country = 'TH'
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.played_at < TIMESTAMP '2025-02-24 09:00:00'
        )
      AND s.plan_or_unknown_2 = 'UNKNOWN'
),
with_activity_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(country_len) OVER (PARTITION BY plan_or_unknown_2, plan_or_unknown), 2) AS plan_or_unknown_2_plan_or_unknown_avg_country_le,
        MAX(country_len) OVER (PARTITION BY plan ORDER BY country_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_country_len_so_far,
        COUNT(*) OVER w_plan_or_unknown_2_plan_or_un AS rows_in_plan_or_unknown_2_plan_or_unknown,
        SUM(country_len) OVER w_plan_or_unknown_2_plan_or_un AS plan_or_unknown_2_plan_or_unknown_sum_country_le,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY country_len DESC, id) AS rn
    FROM with_activity_2
    WINDOW w_plan_or_unknown_2_plan_or_un AS (PARTITION BY plan_or_unknown_2, plan_or_unknown)
),
tagged_2 AS (
    -- Label and score rows
    SELECT *, COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM with_trends
    WHERE plan_or_unknown_2 IN ('premium', 'free')
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, max_country_len_so_far, plan_or_unknown_2_plan_or_unknown_sum_country_le, country_len
    FROM tagged_2
) src
UNPIVOT (
    reading FOR measure IN (max_country_len_so_far, plan_or_unknown_2_plan_or_unknown_sum_country_le, country_len)
) unp
ORDER BY id, measure
