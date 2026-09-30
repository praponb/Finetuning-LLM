WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        ABS(duration_sec - 183) AS duration_sec_distance
    FROM tracks
    WHERE release_date >= DATE '2024-10-23'
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM cleaned_tracks s
    JOIN plays p ON p.track_id = s.id AND p.ms_played < 218129
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_ADD(release_date, 30) AS release_date_plus_30d,
        DATE_ADD(release_date, 7) AS release_date_plus_7d,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM exploded_detail_plays
),
qualified AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'M%'
        )
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM qualified s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        bpm,
        play_id,
        play_source,
        play_ms_played,
        release_date_plus_30d,
        release_date_plus_7d,
        ROUND(AVG(bpm_bucket) OVER (PARTITION BY play_count, DATE_TRUNC('MONTH', release_date) ORDER BY release_date_plus_30d NULLS LAST, id, play_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_bucket_avg_4,
        LEAD(play_ms_played) OVER (PARTITION BY play_count, DATE_TRUNC('MONTH', release_date) ORDER BY release_date_plus_30d NULLS LAST, id, play_id) AS next_play_ms_played,
        RANK() OVER (PARTITION BY artist_id, play_count ORDER BY release_date_plus_7d DESC) AS release_date_plus_7d_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id, play_count ORDER BY release_date_plus_7d DESC, id, play_id) AS seq_no
    FROM summarized_rows
),
with_details_play AS (
    -- Attach play attributes
    SELECT
        s.id,
        s.play_id,
        s.play_source,
        s.play_ms_played,
        s.release_date_plus_30d,
        s.release_date_plus_7d,
        s.next_play_ms_played,
        s.release_date_plus_7d_rank,
        s.seq_no,
        p.played_at AS play_played_at,
        p.source AS play_source_2,
        p.listener_id AS play_listener_id,
        t.is_explicit AS track_is_explicit
    FROM windowed s
    LEFT JOIN plays p ON p.id = s.play_id AND HOUR(p.played_at) BETWEEN 9 AND 12
    LEFT JOIN tracks t ON t.id = p.track_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, metric_value
FROM (
    SELECT id, play_id, next_play_ms_played, release_date_plus_7d_rank, seq_no
    FROM with_details_play
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (next_play_ms_played AS m_next_play_ms_played, release_date_plus_7d_rank AS release_date_plus_7d_rank_v, seq_no AS m_seq_no)
) unp
