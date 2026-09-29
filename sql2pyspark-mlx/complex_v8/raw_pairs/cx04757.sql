WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests,
        INITCAP(TRIM(username)) AS username_clean,
        SIZE(interests) AS interests_count,
        CONCAT_WS('|', username, country) AS username_country_label
    FROM listeners
    WHERE birth_year < 1994
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        signup_date,
        birth_year,
        interests,
        username_clean,
        interests_count,
        birth_year + interests_count AS birth_year_plus_interests_count,
        SIZE(interests) AS interests_count_2
    FROM cleaned
    WHERE birth_year BETWEEN 1968 AND 1994
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.id) AS play_count,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.listener_id = s.id) AS min_play_ms_played
    FROM tagged s
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.signup_date,
        s.username_clean,
        s.interests_count,
        s.birth_year_plus_interests_count,
        s.interests_count_2,
        s.play_count,
        s.min_play_ms_played,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM summarized_rows s
    LEFT JOIN plays p ON p.listener_id = s.id
)
SELECT
    play_device,
    play_id,
    play_source,
    ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list,
    COUNT(*) AS cnt,
    MIN(interests_count) AS min_interests_count
FROM with_children
GROUP BY ROLLUP(play_device, play_id, play_source)
ORDER BY play_device NULLS LAST, play_id NULLS LAST, play_source NULLS LAST
