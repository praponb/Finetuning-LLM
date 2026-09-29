WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM plays
    WHERE source <> 'radio'
      AND ms_played > 56858
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        t.artist_id AS track_artist_id
    FROM base_plays s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        ms_played,
        source,
        track_artist_id,
        RANK() OVER w_source_track_artist_id_by_pl AS played_at_rank,
        ROW_NUMBER() OVER w_source_track_artist_id_by_ms AS seq_no,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        ROUND(PERCENT_RANK() OVER w_source_track_artist_id_by_pl_2, 3) AS played_at_percent_rank,
        SUM(ms_played) OVER (PARTITION BY source ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY source, track_artist_id ORDER BY played_at), 3) AS played_at_cume_dist,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY listener_id)) AS distinct_source_in_listener_id,
        NTILE(3) OVER w_source_by_ms_played AS ms_played_tercile,
        ROUND(AVG(ms_played) OVER (PARTITION BY source), 2) AS source_avg_ms_played,
        LAG(played_at, 2) OVER (PARTITION BY source ORDER BY ms_played ASC, id) AS prev2_played_at,
        RANK() OVER (ORDER BY played_at, id) AS played_at_rank_2,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY TO_DATE(played_at) ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(played_at) ORDER BY ms_played DESC, id) AS ms_played_seq
    FROM enriched_track
    WINDOW w_source_track_artist_id_by_pl AS (PARTITION BY source, track_artist_id ORDER BY played_at, id), w_source_track_artist_id_by_ms AS (PARTITION BY source, track_artist_id ORDER BY ms_played DESC, id), w_source_track_artist_id_by_pl_2 AS (PARTITION BY source, track_artist_id ORDER BY played_at), w_source_by_ms_played AS (PARTITION BY source ORDER BY ms_played ASC, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        ms_played,
        source,
        track_artist_id,
        seq_no,
        rows_in_listener_id,
        played_at_percent_rank,
        ms_played_sum_3_prior,
        played_at_cume_dist,
        distinct_source_in_listener_id,
        ms_played_tercile,
        played_at_rank_2,
        second_ms_played,
        ROUND(played_at_rank_2 - AVG(played_at_rank_2) OVER (PARTITION BY seq_no, ms_played_seq), 2) AS played_at_rank_2_vs_seq_no_ms_played_seq_avg,
        MAX(ms_played_seq) OVER (PARTITION BY seq_no, ms_played_seq) AS seq_no_ms_played_seq_max_ms_played_seq,
        RANK() OVER w_played_at_rank_2_played_at_r AS seq_no_rank,
        LAG(ms_played_sum_3_prior, 1, 0) OVER (ORDER BY source_avg_ms_played NULLS LAST, id) AS prev_ms_played_sum_3_prior,
        COUNT(*) OVER w_seq_no_ms_played_seq AS rows_in_seq_no_ms_played_seq,
        NTH_VALUE(ms_played_tercile, 2) OVER (PARTITION BY played_at_rank_2, played_at_rank ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_tercile,
        DENSE_RANK() OVER (ORDER BY source_avg_ms_played NULLS LAST) AS source_avg_ms_played_dense_rank,
        NTH_VALUE(ms_played_vs_source_avg, 2) OVER (PARTITION BY played_at_rank_2, played_at_rank ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_vs_source_avg
    FROM metrics
    WINDOW w_played_at_rank_2_played_at_r AS (PARTITION BY played_at_rank_2, played_at_rank ORDER BY seq_no DESC), w_seq_no_ms_played_seq AS (PARTITION BY seq_no, ms_played_seq)
)
SELECT
    id,
    listener_id,
    ms_played,
    track_artist_id,
    rows_in_listener_id,
    ms_played_sum_3_prior,
    played_at_cume_dist,
    distinct_source_in_listener_id,
    ms_played_tercile,
    played_at_rank_2,
    second_ms_played,
    seq_no_ms_played_seq_max_ms_played_seq,
    seq_no_rank,
    rows_in_seq_no_ms_played_seq,
    second_ms_played_tercile,
    source_avg_ms_played_dense_rank,
    second_ms_played_vs_source_avg
FROM windowed
ORDER BY ms_played, track_artist_id DESC
