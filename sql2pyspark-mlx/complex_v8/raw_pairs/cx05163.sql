WITH prepared_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year
    FROM listeners
    WHERE YEAR(signup_date) = 2025
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.plan,
        s.country,
        s.signup_date,
        s.birth_year,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM prepared_listeners s
    JOIN plays p ON p.listener_id = s.id
)
SELECT
    play_device,
    play_id,
    country,
    ROUND(STDDEV(birth_year), 2) AS stddev_birth_year,
    COUNT(*) AS row_count
FROM detail_rows_plays
GROUP BY CUBE(play_device, play_id, country)
ORDER BY play_device NULLS LAST, play_id NULLS LAST, country NULLS LAST
