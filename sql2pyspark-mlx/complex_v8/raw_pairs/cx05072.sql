WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        birth_year
    FROM listeners
    WHERE signup_date IS NOT NULL
      AND LENGTH(TRIM(username)) >= 4
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.birth_year,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.track_id AS play_track_id,
        p.source AS play_source
    FROM valid s
    JOIN plays p ON p.listener_id = s.id
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.birth_year,
        s.play_id,
        s.play_device,
        s.play_track_id,
        s.play_source,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title,
        p.ms_played AS play_ms_played
    FROM exploded_detail s
    JOIN tracks t ON t.id = s.play_track_id
    JOIN plays p ON p.id = s.play_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        birth_year,
        play_id,
        play_device,
        play_track_id,
        play_source,
        track_is_explicit,
        track_title,
        play_ms_played,
        ROUND(play_ms_played - AVG(play_ms_played) OVER (PARTITION BY play_track_id, play_id), 2) AS play_ms_played_vs_play_track_id_play_id_avg,
        LAG(birth_year) OVER w_play_id_track_is_explicit_by AS prev_birth_year,
        ROUND(PERCENT_RANK() OVER w_play_id_track_is_explicit_by_2, 3) AS birth_year_percent_rank,
        ROUND(AVG(play_ms_played) OVER w_play_track_id_play_id, 2) AS play_track_id_play_id_avg_play_ms_played,
        RANK() OVER (PARTITION BY play_id, track_is_explicit ORDER BY birth_year ASC, id, play_id) AS birth_year_rank
    FROM joined_track
    WINDOW w_play_id_track_is_explicit_by AS (PARTITION BY play_id, track_is_explicit ORDER BY birth_year ASC, id, play_id), w_play_id_track_is_explicit_by_2 AS (PARTITION BY play_id, track_is_explicit ORDER BY birth_year ASC), w_play_track_id_play_id AS (PARTITION BY play_track_id, play_id)
),
play_source_stats AS (
    -- Max prev_birth_year per play_source
    SELECT play_source, MAX(prev_birth_year) AS max_prev_birth_year
    FROM windowed
    GROUP BY play_source
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prev_birth_year,
        ROUND(s.prev_birth_year - g.max_prev_birth_year, 2) AS prev_birth_year_minus_max
    FROM windowed s
    JOIN play_source_stats g ON g.play_source = s.play_source
    WHERE s.prev_birth_year = g.max_prev_birth_year
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(prev_birth_year_minus_max) IGNORE NULLS OVER (ORDER BY birth_year DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_birth_year_minus_max,
        MAX(birth_year_rank) OVER (PARTITION BY track_is_explicit) AS track_is_explicit_max_birth_year_rank,
        ROUND(prev_birth_year / NULLIF(SUM(prev_birth_year) OVER w_track_is_explicit, 0), 4) AS prev_birth_year_share_of_track_is_explicit,
        COUNT(*) OVER (PARTITION BY track_is_explicit) AS rows_in_track_is_explicit,
        ROW_NUMBER() OVER w_play_source_by_prev_birth_ye AS rn
    FROM compared
    WINDOW w_track_is_explicit AS (PARTITION BY track_is_explicit), w_play_source_by_prev_birth_ye AS (PARTITION BY play_source ORDER BY prev_birth_year ASC, id, play_id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(prev_birth_year) OVER w_birth_year_rank AS birth_year_rank_max_prev_birth_year,
        LAST_VALUE(prev_birth_year_minus_max) IGNORE NULLS OVER (PARTITION BY play_track_id, play_device ORDER BY prev_birth_year DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_birth_year_minus_max_2,
        SIZE(COLLECT_SET(play_device) OVER (PARTITION BY birth_year_rank)) AS distinct_play_device_in_birth_year_rank,
        LAST_VALUE(max_prev_birth_year) OVER (PARTITION BY play_track_id, play_device ORDER BY prev_birth_year DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_prev_birth_year,
        FIRST_VALUE(rows_in_track_is_explicit) OVER (PARTITION BY play_track_id, play_device ORDER BY prev_birth_year DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rows_in_track_is_explicit
    FROM windowed_2
    WINDOW w_birth_year_rank AS (PARTITION BY birth_year_rank)
)
SELECT
    id,
    birth_year,
    play_id,
    play_device,
    play_track_id,
    play_source,
    track_is_explicit,
    track_title,
    play_ms_played,
    prev_birth_year,
    birth_year_percent_rank,
    max_prev_birth_year,
    prev_birth_year_minus_max,
    track_is_explicit_max_birth_year_rank,
    prev_birth_year_share_of_track_is_explicit,
    rn,
    birth_year_rank_max_prev_birth_year,
    last_known_prev_birth_year_minus_max_2,
    last_max_prev_birth_year,
    birth_year_percent_rank + rn AS birth_year_percent_rank_plus_rn,
    CASE WHEN play_source IN ('radio', 'album') THEN 'group_a' WHEN play_source = 'playlist' THEN 'group_b' ELSE 'other' END AS play_source_group
FROM sequenced
WHERE play_ms_played_vs_play_track_id_play_id_avg <= 240000.0
ORDER BY play_device
