WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DAYOFWEEK(played_at) AS played_at_dayofweek,
        LENGTH(TRIM(source)) AS source_len,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE is_skipped = TRUE
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        played_at_dayofweek,
        source_len,
        source_prefix,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM scoped
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(played_at_year) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_year,
        MIN(source_len) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_source_len_so_far,
        ROW_NUMBER() OVER w_played_at_dayofweek_played_a AS seq_no
    FROM scored
    WINDOW w_played_at_dayofweek_played_a AS (PARTITION BY played_at_dayofweek, YEAR(played_at) ORDER BY played_at DESC, id)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count, MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.min_play_ms_played
    FROM with_windows s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
-- Subtotals with GROUPING SETS
SELECT
    seq_no,
    play_count,
    played_at_dayofweek,
    MAX(min_source_len_so_far) AS max_min_source_len_so_far
FROM summarized_rows
GROUP BY GROUPING SETS ((seq_no, play_count, played_at_dayofweek), (seq_no), ())
ORDER BY seq_no NULLS LAST, play_count NULLS LAST, played_at_dayofweek NULLS LAST
