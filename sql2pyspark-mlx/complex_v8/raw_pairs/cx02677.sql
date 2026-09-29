WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_TRUNC('QUARTER', signup_date) AS signup_date_quarter
    FROM listeners
),
deduped AS (
    -- Keep the latest row per plan
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        signup_date_quarter
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY plan ORDER BY birth_year DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.signup_date,
        s.birth_year,
        s.interests,
        s.signup_date_quarter,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM deduped s
    LEFT JOIN plays p ON p.listener_id = s.id
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.signup_date,
        s.birth_year,
        s.signup_date_quarter,
        s.play_id,
        s.play_ms_played,
        s.play_source,
        t.interest
    FROM exploded_detail s
    LATERAL VIEW OUTER EXPLODE(s.interests) t AS interest
)
-- Final projection
SELECT
    id,
    username,
    birth_year,
    play_id,
    play_ms_played,
    play_source,
    interest,
    ROUND(birth_year / NULLIF(play_ms_played, 0), 3) AS birth_year_per_play_ms_played,
    CONCAT_WS(' / ', interest, username) AS interest_username_label
FROM exploded
WHERE play_ms_played < 324437
ORDER BY birth_year DESC, username DESC
