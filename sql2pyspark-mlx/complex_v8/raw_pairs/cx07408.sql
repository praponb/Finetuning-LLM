WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CASE WHEN device IN ('car', 'speaker') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
),
enriched AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.device_group,
        s.source_or_unknown,
        t.title AS track_title
    FROM recent s
    JOIN tracks t ON t.id = s.track_id
    WHERE UPPER(TRIM(t.title)) LIKE 'N%'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(played_at) AS played_at_year,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        SUBSTRING(TRIM(track_title), 1, 4) AS track_title_prefix
    FROM enriched
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY is_skipped, played_at_year ORDER BY played_at DESC, id) AS played_at_quintile,
        RANK() OVER (PARTITION BY is_skipped, played_at_year ORDER BY played_at DESC, id) AS played_at_rank,
        LAST_VALUE(played_at_year) IGNORE NULLS OVER (PARTITION BY is_skipped, played_at_year ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_year,
        ROUND(CUME_DIST() OVER w_track_id_is_skipped_by_playe, 3) AS played_at_year_cume_dist,
        MAX(played_at_year) OVER w_listener_id_source_or_unknow AS listener_id_source_or_unknown_max_played_at_year,
        DENSE_RANK() OVER (PARTITION BY is_skipped, played_at_year ORDER BY played_at DESC, id) AS played_at_dense_rank
    FROM scored
    WINDOW w_track_id_is_skipped_by_playe AS (PARTITION BY track_id, is_skipped ORDER BY played_at_year), w_listener_id_source_or_unknow AS (PARTITION BY listener_id, source_or_unknown)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY device_group ORDER BY played_at NULLS LAST, id) AS played_at_quartile,
        FIRST_VALUE(last_known_played_at_year) OVER (PARTITION BY device_group ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_known_played_at_year,
        ROW_NUMBER() OVER w_all_by_played_at AS played_at_seq,
        ROUND(AVG(played_at_year) OVER (ORDER BY played_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS played_at_year_avg_2,
        MAX(played_at_dense_rank) OVER (PARTITION BY played_at_dense_rank) AS played_at_dense_rank_max_played_at_dense_rank,
        LAST_VALUE(played_at_rank) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_rank
    FROM metrics
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.last_played_at
    FROM sequenced s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    is_skipped,
    device_group,
    source_or_unknown,
    track_title,
    played_at_year,
    track_title_prefix,
    played_at_quintile,
    played_at_rank,
    last_known_played_at_year,
    played_at_year_cume_dist,
    listener_id_source_or_unknown_max_played_at_year,
    played_at_dense_rank,
    played_at_quartile,
    played_at_seq,
    played_at_year_avg_2,
    played_at_dense_rank_max_played_at_dense_rank,
    last_known_played_at_rank,
    play_count,
    last_played_at,
    DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label_2
FROM with_stats
ORDER BY track_id, last_played_at
