WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        YEAR(played_at) AS played_at_year,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 14
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.played_at_year,
        s.is_skipped_label,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec,
        t.bpm AS track_bpm,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM base_plays s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_is_skipped_by_played_at_ties, 3) AS played_at_cume_dist,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY track_id), 0), 4) AS track_duration_sec_share_of_track_id,
        MIN(played_at_year) OVER (PARTITION BY track_id) AS track_id_min_played_at_year,
        SUM(played_at_year) OVER w_listener_id AS listener_id_sum_played_at_year,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        track_duration_sec - LAG(track_duration_sec) OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id) AS track_duration_sec_change,
        MAX(track_bpm) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS track_bpm_max_6_prior,
        LEAD(played_at_year, 3) OVER (PARTITION BY is_skipped, is_skipped_label ORDER BY track_duration_sec, id) AS next3_played_at_year
    FROM joined_track
    WINDOW w_is_skipped_by_played_at_ties AS (PARTITION BY is_skipped ORDER BY played_at DESC), w_listener_id AS (PARTITION BY listener_id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(artist_country) OVER w_is_skipped_label_next3_playe) AS distinct_artist_country_in_is_skipped_label_next,
        NTILE(3) OVER (PARTITION BY next3_played_at_year, track_id ORDER BY track_bpm DESC, id) AS track_bpm_tercile,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER w_is_skipped_label_next3_playe, 0), 4) AS track_duration_sec_share_of_is_skipped_label_nex,
        ROUND(track_duration_sec_change / NULLIF(SUM(track_duration_sec_change) OVER (PARTITION BY is_skipped_label, next3_played_at_year), 0), 4) AS track_duration_sec_change_share_of_is_skipped_la,
        LAG(next3_played_at_year, 2) OVER (ORDER BY track_duration_sec_share_of_track_id DESC, id) AS prev2_next3_played_at_year
    FROM with_trends
    WINDOW w_is_skipped_label_next3_playe AS (PARTITION BY is_skipped_label, next3_played_at_year)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_artist_country_in_is_skipped_label_next,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows
    FROM metrics
    GROUP BY distinct_artist_country_in_is_skipped_label_next
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT distinct_artist_country_in_is_skipped_label_next, distinct_ids
FROM grouped
ORDER BY distinct_ids, distinct_artist_country_in_is_skipped_label_next NULLS LAST
LIMIT 10
