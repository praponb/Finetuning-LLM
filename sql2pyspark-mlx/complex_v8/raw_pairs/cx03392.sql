WITH recent_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        plan,
        country,
        birth_year,
        interests
    FROM listeners
    WHERE username IS NOT NULL
      AND plan = 'student'
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.country,
        s.birth_year,
        s.interests,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.played_at AS play_played_at
    FROM recent_listeners s
    JOIN plays p ON p.listener_id = s.id AND p.device <> 'phone'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        country,
        interests,
        play_id,
        play_device,
        play_source,
        play_played_at,
        birth_year - LAG(birth_year) OVER (PARTITION BY play_device, country ORDER BY play_played_at, id, play_id) AS birth_year_change,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY MONTH(play_played_at), play_device), 0), 4) AS birth_year_share_of_play_played_at_month_play_de
    FROM line_level
)
-- Subtotals with GROUPING SETS
SELECT
    play_id,
    country,
    GROUPING_ID() AS gid,
    ROUND(SUM(birth_year_share_of_play_played_at_month_play_de), 2) AS total_birth_year_share_of_play_played_at_month_p,
    COUNT(DISTINCT play_id) AS distinct_play_ids
FROM sequenced
GROUP BY GROUPING SETS ((play_id, country), (play_id), (country))
