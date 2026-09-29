WITH cleaned AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        CONCAT(country, '#', CAST(birth_year AS STRING)) AS country_ref
    FROM listeners
    WHERE plan = 'premium'
      AND country = 'BR'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        signup_date,
        interests,
        MONTH(signup_date) AS signup_date_month,
        CASE WHEN birth_year >= 1990 THEN 'large' WHEN birth_year >= 1975 THEN 'medium' ELSE 'small' END AS birth_year_tier,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek
    FROM cleaned
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM tagged s
    LEFT JOIN play_stats p ON p.listener_id = s.id
)
SELECT
    signup_date_dayofweek,
    birth_year_tier,
    play_count,
    ROUND(SUM(play_count) / NULLIF(SUM(signup_date_month), 0), 3) AS play_count_to_signup_date_month_ratio,
    ROUND(STDDEV(signup_date_dayofweek), 2) AS stddev_signup_date_dayofweek
FROM profiled
GROUP BY GROUPING SETS ((signup_date_dayofweek, birth_year_tier, play_count), (signup_date_dayofweek), (birth_year_tier), ())
