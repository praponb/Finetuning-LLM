WITH base_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        birth_year
    FROM listeners
    WHERE LOWER(username) LIKE '%no%'
      AND signup_date < DATE '2025-05-05'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN birth_year >= 1980 THEN 'large' WHEN birth_year >= 1968 THEN 'medium' ELSE 'small' END AS birth_year_tier,
        CONCAT(username, '#', CAST(id AS STRING)) AS username_ref
    FROM base_listeners
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.track_id >= 24
              AND HOUR(p.played_at) BETWEEN 6 AND 11
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        country,
        birth_year,
        username_ref,
        MAX(birth_year) OVER (PARTITION BY plan, birth_year_tier) AS plan_birth_year_tier_max_birth_year,
        LAST_VALUE(birth_year) IGNORE NULLS OVER (PARTITION BY country, birth_year_tier ORDER BY birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_birth_year,
        COUNT(*) OVER w_plan_birth_year_tier AS rows_in_plan_birth_year_tier,
        RANK() OVER w_country_birth_year_tier_by_b_2 AS birth_year_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY country, birth_year_tier ORDER BY birth_year DESC), 3) AS birth_year_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY country, birth_year_tier ORDER BY birth_year DESC, id) AS rn
    FROM qualified
    WINDOW w_plan_birth_year_tier AS (PARTITION BY plan, birth_year_tier), w_country_birth_year_tier_by_b_2 AS (PARTITION BY country, birth_year_tier ORDER BY birth_year DESC)
)
SELECT id, metric_name, reading
FROM (
    SELECT id, birth_year, plan_birth_year_tier_max_birth_year, rows_in_plan_birth_year_tier, birth_year_rank
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric_name IN (birth_year, plan_birth_year_tier_max_birth_year, rows_in_plan_birth_year_tier, birth_year_rank)
) unp
