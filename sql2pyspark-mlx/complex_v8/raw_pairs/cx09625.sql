WITH valid_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests,
        SUBSTRING(TRIM(country), 1, 4) AS country_prefix,
        SUBSTRING(TRIM(username), 1, 2) AS username_prefix
    FROM listeners
    WHERE plan IN ('free', 'student')
      AND country IN ('UK', 'KR')
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM valid_listeners s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.track_id >= 21
              AND p.device IN ('car', 'speaker')
        )
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', signup_date) AS days_since_signup_date,
        QUARTER(signup_date) AS signup_date_quarter,
        CONCAT_WS(': ', username, username_prefix) AS username_username_prefix_label
    FROM with_activity
    WHERE username IS NOT NULL
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.plan,
        s.signup_date,
        s.interests,
        s.username_prefix,
        s.days_since_signup_date,
        s.signup_date_quarter,
        s.username_username_prefix_label,
        p.id AS play_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM scored s
    LEFT JOIN plays p ON p.listener_id = s.id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(play_ms_played) OVER (PARTITION BY plan, play_device ORDER BY signup_date DESC, id, play_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS play_ms_played_avg_3_prior,
        ROUND(AVG(signup_date_quarter) OVER (PARTITION BY plan, play_device ORDER BY signup_date DESC, id, play_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS signup_date_quarter_avg_6_prior,
        ROUND(signup_date_quarter / NULLIF(SUM(signup_date_quarter) OVER (PARTITION BY play_source, play_device), 0), 4) AS signup_date_quarter_share_of_play_source_play_de
    FROM detail_rows_plays
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.plan,
        s.interests,
        s.username_prefix,
        s.days_since_signup_date,
        s.signup_date_quarter,
        s.username_username_prefix_label,
        s.play_id,
        s.play_device,
        s.play_ms_played,
        s.play_ms_played_avg_3_prior,
        s.signup_date_quarter_avg_6_prior,
        s.signup_date_quarter_share_of_play_source_play_de,
        p.id AS play_id_2,
        p.source AS play_source_2,
        p.is_skipped AS play_is_skipped_2
    FROM windowed s
    JOIN plays p ON p.listener_id = s.id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM exploded_detail_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.id
)
SELECT *
FROM (
    SELECT play_device, play_id_2, plan, signup_date_quarter
    FROM profiled
) src
PIVOT (
    MIN(signup_date_quarter)
    FOR plan IN ('family', 'free')
)
ORDER BY play_device, play_id_2
