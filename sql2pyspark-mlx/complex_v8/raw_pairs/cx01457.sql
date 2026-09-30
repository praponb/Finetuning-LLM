WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        UPPER(TRIM(device)) AS device_clean,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE source = 'album'
      AND listener_id <= 4
),
filtered AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM recent_plays s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND NOT t.is_explicit
        )
),
source_stats AS (
    -- Avg device_len per source
    SELECT source, AVG(device_len) AS avg_device_len
    FROM filtered
    GROUP BY source
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_device_len,
        ROUND(s.device_len - g.avg_device_len, 2) AS device_len_minus_avg
    FROM filtered s
    JOIN source_stats g ON g.source = s.source
    WHERE s.device_len > g.avg_device_len
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        played_at,
        source,
        device_source_label,
        device_clean,
        avg_device_len,
        device_len_minus_avg,
        ROUND(avg_device_len - AVG(avg_device_len) OVER (PARTITION BY device_len, device), 2) AS avg_device_len_vs_device_len_device_avg,
        DENSE_RANK() OVER (ORDER BY device_len DESC) AS device_len_dense_rank,
        NTH_VALUE(avg_device_len, 2) OVER (ORDER BY device_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_device_len
    FROM with_activity
    WINDOW w_device_len_device AS (PARTITION BY device_len, device)
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.track_id,
        s.played_at,
        s.source,
        s.device_source_label,
        s.device_clean,
        s.avg_device_len,
        s.device_len_minus_avg,
        s.device_len_dense_rank,
        s.second_avg_device_len,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec
    FROM with_trends s
    LEFT JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(avg_device_len) OVER (PARTITION BY track_id, device_len_dense_rank), 2) AS track_id_device_len_dense_rank_avg_avg_device_le,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id, device_len_dense_rank)) AS distinct_source_in_track_id_device_len_dense_ran,
        SUM(second_avg_device_len) OVER (PARTITION BY track_id, device_len_dense_rank) AS track_id_device_len_dense_rank_sum_second_avg_de,
        LAST_VALUE(track_release_date) OVER (PARTITION BY track_id, device_len_dense_rank ORDER BY track_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_release_date,
        ROUND(device_len_minus_avg - AVG(device_len_minus_avg) OVER (PARTITION BY track_id, device_len_dense_rank), 2) AS device_len_minus_avg_vs_track_id_device_len_dens,
        SUM(avg_device_len) OVER (PARTITION BY track_id, device_len_dense_rank ORDER BY track_duration_sec ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS avg_device_len_sum_1_prior
    FROM joined
)
SELECT
    id,
    track_id,
    played_at,
    source,
    device_source_label,
    device_clean,
    device_len_minus_avg,
    second_avg_device_len,
    track_release_date,
    track_id_device_len_dense_rank_sum_second_avg_de,
    last_track_release_date,
    device_len_minus_avg_vs_track_id_device_len_dens,
    DATEDIFF(track_release_date, last_track_release_date) AS days_last_track_release_date_to_track_release_da,
    COALESCE(avg_device_len, 0.0) AS avg_device_len_filled
FROM metrics
ORDER BY played_at DESC, id
LIMIT 5
