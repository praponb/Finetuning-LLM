WITH valid_listeners AS (
    -- Filter listeners
    SELECT
        id,
        plan,
        country,
        signup_date,
        interests
    FROM listeners
    WHERE birth_year >= 1967
      AND SIZE(interests) >= 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        signup_date,
        interests,
        DATE_ADD(signup_date, 365) AS signup_date_plus_365d
    FROM valid_listeners
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at,
        (SELECT COUNT(CASE WHEN p.device IN ('desktop', 'car', 'tv') THEN 1 END) FROM plays p WHERE p.listener_id = s.id) AS play_device_count
    FROM tagged s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(play_device_count) OVER (PARTITION BY play_device_count, DATE_TRUNC('MONTH', signup_date_plus_365d) ORDER BY signup_date ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS play_device_count_avg_3,
        NTILE(3) OVER w_play_device_count_by_last_pl AS last_played_at_tercile
    FROM summarized_rows
    WINDOW w_play_device_count_by_last_pl AS (PARTITION BY play_device_count ORDER BY last_played_at ASC, id)
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        *,
        play_device_count_avg_3 - last_played_at_tercile AS play_device_count_avg_3_minus_last_played_at_ter
    FROM sequenced
)
-- Final projection
SELECT
    id,
    signup_date,
    last_played_at,
    play_device_count,
    play_device_count_avg_3,
    last_played_at_tercile,
    TO_DATE(last_played_at) AS last_played_at_date
FROM tagged_2
ORDER BY last_played_at DESC, id
LIMIT 10
