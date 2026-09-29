WITH filtered AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        birth_year,
        interests,
        ABS(birth_year - 1981) AS birth_year_distance,
        ABS(birth_year - 1975) AS birth_year_distance_2,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM listeners
    WHERE birth_year >= 1968
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM filtered s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
enriched AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(country_or_unknown)) AS country_or_unknown_clean
    FROM summarized_rows
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        birth_year,
        interests,
        birth_year_distance,
        country_or_unknown,
        first_played_at,
        SUBSTRING(TRIM(plan), 1, 3) AS plan_prefix
    FROM enriched
),
flattened AS (
    -- One row per interest
    SELECT
        s.id,
        s.birth_year,
        s.birth_year_distance,
        s.country_or_unknown,
        s.first_played_at,
        s.plan_prefix,
        t.interest_pos,
        t.interest
    FROM tagged s
    LATERAL VIEW OUTER POSEXPLODE(s.interests) t AS interest_pos, interest
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM flattened
    WHERE birth_year_distance > (SELECT AVG(birth_year_distance) FROM flattened)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        birth_year - LAG(birth_year) OVER (ORDER BY birth_year ASC, id, interest_pos) AS birth_year_change,
        LAG(birth_year) OVER (PARTITION BY country_or_unknown ORDER BY birth_year, id, interest_pos) AS prev_birth_year,
        ROUND(birth_year_distance - AVG(birth_year_distance) OVER w_interest, 2) AS birth_year_distance_vs_interest_avg
    FROM with_activity
    WINDOW w_interest AS (PARTITION BY interest)
)
-- Final projection
SELECT
    id,
    birth_year_distance,
    country_or_unknown,
    plan_prefix,
    interest,
    birth_year_change,
    prev_birth_year,
    GREATEST(birth_year, birth_year_change) AS greatest_birth_year_birth_year_change
FROM metrics
ORDER BY prev_birth_year DESC, birth_year_distance
