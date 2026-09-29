WITH cleaned_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        plan,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE plan IN ('student', 'free')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(plan), ' ', '_')) AS plan_slug,
        DATEDIFF(DATE '2025-06-30', signup_date) AS days_since_signup_date
    FROM cleaned_listeners
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.signup_date,
        s.plan_slug,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM enriched s
    JOIN plays p ON p.listener_id = s.id AND p.is_skipped = FALSE
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY play_id ORDER BY signup_date, id, play_id) AS rn,
        SUM(play_ms_played) OVER (PARTITION BY play_is_skipped) AS play_is_skipped_sum_play_ms_played,
        ROUND(play_ms_played / NULLIF(SUM(play_ms_played) OVER (PARTITION BY play_is_skipped), 0), 4) AS play_ms_played_share_of_play_is_skipped,
        DATEDIFF(signup_date, LAG(signup_date) OVER (PARTITION BY play_id ORDER BY signup_date, id, play_id)) AS days_since_prev_signup_date,
        LEAD(signup_date, 3) OVER (PARTITION BY play_id ORDER BY signup_date, id, play_id) AS next3_signup_date
    FROM exploded_detail_plays
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.signup_date,
        s.plan_slug,
        s.play_id,
        s.play_is_skipped,
        s.play_ms_played_share_of_play_is_skipped,
        s.next3_signup_date,
        p.id AS play_id_2,
        p.source AS play_source,
        p.device AS play_device,
        p.ms_played AS play_ms_played_2
    FROM with_windows s
    JOIN plays p ON p.listener_id = s.id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        signup_date,
        play_id,
        play_ms_played_share_of_play_is_skipped,
        next3_signup_date,
        play_id_2,
        play_source,
        play_device,
        play_ms_played_2,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        LOWER(REGEXP_REPLACE(TRIM(play_source), ' ', '_')) AS play_source_slug,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek
    FROM exploded_detail
)
-- Final projection
SELECT
    id,
    signup_date,
    play_ms_played_share_of_play_is_skipped,
    next3_signup_date,
    play_id_2,
    play_source,
    play_device,
    play_ms_played_2
FROM scored
ORDER BY play_ms_played_share_of_play_is_skipped DESC
