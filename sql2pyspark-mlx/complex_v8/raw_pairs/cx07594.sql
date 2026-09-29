WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        CONCAT_WS('|', device, source) AS device_source_label,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE device NOT IN ('tv', 'desktop')
      AND is_skipped = FALSE
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.moods AS track_moods,
        t.release_date AS track_release_date,
        t.artist_id AS track_artist_id
    FROM base_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.artist_id <= 13
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(release_date) AS first_track_release_date,
        SUM(duration_sec) AS total_track_duration_sec,
        MAX(release_date) AS last_track_release_date
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date,
        t.total_track_duration_sec,
        t.last_track_release_date
    FROM with_details_track s
    LEFT JOIN track_stats t ON t.artist_id = s.track_artist_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        device_source_label,
        played_at_year,
        track_artist_id,
        first_track_release_date,
        last_track_release_date,
        LAG(total_track_duration_sec, 1, 0) OVER (ORDER BY played_at, id) AS prev_total_track_duration_sec,
        SUM(total_track_duration_sec) OVER (PARTITION BY track_id, is_skipped) AS track_id_is_skipped_sum_total_track_duration_sec,
        COUNT(*) OVER (PARTITION BY track_id, is_skipped) AS rows_in_track_id_is_skipped,
        ROUND(AVG(total_track_duration_sec) OVER (PARTITION BY track_id, is_skipped), 2) AS track_id_is_skipped_avg_total_track_duration_sec,
        ROUND(total_track_duration_sec / NULLIF(SUM(total_track_duration_sec) OVER w_track_id_is_skipped, 0), 4) AS total_track_duration_sec_share_of_track_id_is_sk,
        SIZE(COLLECT_SET(track_artist_id) OVER (PARTITION BY track_id, is_skipped)) AS distinct_track_artist_id_in_track_id_is_skipped
    FROM profiled
    WINDOW w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped)
),
screened AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND NOT ARRAY_CONTAINS(l.interests, 'jazz')
              AND UPPER(TRIM(l.username)) LIKE 'F%'
        )
),
listener_id_stats AS (
    -- Avg prev_total_track_duration_sec per listener_id
    SELECT listener_id, AVG(prev_total_track_duration_sec) AS avg_prev_total_track_duration_sec
    FROM screened
    GROUP BY listener_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prev_total_track_duration_sec,
        ROUND(s.prev_total_track_duration_sec - g.avg_prev_total_track_duration_sec, 2) AS prev_total_track_duration_sec_minus_avg
    FROM screened s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.prev_total_track_duration_sec > g.avg_prev_total_track_duration_sec
)
SELECT
    id,
    listener_id,
    played_at,
    device_source_label,
    track_artist_id,
    first_track_release_date,
    track_id_is_skipped_avg_total_track_duration_sec,
    distinct_track_artist_id_in_track_id_is_skipped,
    avg_prev_total_track_duration_sec,
    prev_total_track_duration_sec_minus_avg,
    DATE_TRUNC('DAY', played_at) AS played_at_day,
    DATE_TRUNC('DAY', played_at_year) AS played_at_year_day
FROM outliers
ORDER BY track_artist_id NULLS LAST, device_source_label DESC NULLS FIRST
