WITH scoped_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        CONCAT(plan, '#', CAST(birth_year AS STRING)) AS plan_ref
    FROM listeners
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.plan,
        s.birth_year,
        s.interests,
        s.plan_ref,
        p.id AS play_id,
        p.device AS play_device,
        p.ms_played AS play_ms_played
    FROM scoped_listeners s
    LEFT JOIN plays p ON p.listener_id = s.id
),
combined_details_play AS (
    -- Join plays
    SELECT
        s.id,
        s.plan,
        s.interests,
        s.plan_ref,
        s.play_id,
        s.play_device,
        p.played_at AS play_played_at,
        l.birth_year AS listener_birth_year
    FROM detail_rows_plays s
    LEFT JOIN plays p ON p.id = s.play_id
    LEFT JOIN listeners l ON l.id = p.listener_id
),
with_details_play AS (
    -- Attach play attributes
    SELECT
        s.*,
        p.track_id AS play_track_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM combined_details_play s
    LEFT JOIN plays p ON p.id = s.play_id
)
SELECT *
FROM (
    SELECT play_id, plan, play_ms_played
    FROM with_details_play
) src
PIVOT (
    MIN(play_ms_played) AS min_v, COUNT(play_ms_played) AS n
    FOR plan IN ('family', 'premium', 'free')
)
ORDER BY play_id
