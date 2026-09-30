WITH scoped AS (
    -- Filter listeners
    SELECT
        id,
        username,
        signup_date,
        birth_year
    FROM listeners
    WHERE country NOT IN ('TH', 'KR')
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.signup_date,
        s.birth_year,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM scoped s
    LEFT JOIN plays p ON p.listener_id = s.id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM exploded_detail_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
play_track_id_stats AS (
    -- Median play_ms_played per play_track_id
    SELECT play_track_id, PERCENTILE_APPROX(play_ms_played, 0.5) AS median_play_ms_played
    FROM summarized_rows
    GROUP BY play_track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_play_ms_played,
        ROUND(s.play_ms_played - g.median_play_ms_played, 2) AS play_ms_played_minus_median
    FROM summarized_rows s
    JOIN play_track_id_stats g ON g.play_track_id = s.play_track_id
    WHERE s.play_ms_played >= g.median_play_ms_played
)
-- Final projection
SELECT
    id,
    signup_date,
    birth_year,
    play_ms_played,
    play_count,
    median_play_ms_played,
    play_ms_played_minus_median
FROM compared
WHERE median_play_ms_played <= 242000.0
ORDER BY median_play_ms_played DESC NULLS FIRST
