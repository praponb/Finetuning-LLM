WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE duration_sec BETWEEN 144 AND 344
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM valid_tracks
    WHERE NOT is_explicit
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        duration_sec_filled - LAG(duration_sec_filled) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS duration_sec_filled_change,
        RANK() OVER (ORDER BY duration_sec_filled DESC) AS duration_sec_filled_rank,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        MIN(bpm_per_duration_sec) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_per_duration_sec_so_far,
        NTILE(4) OVER w_is_explicit_by_release_date AS release_date_quartile,
        SUM(bpm_per_duration_sec) OVER (PARTITION BY artist_id, is_explicit_label) AS artist_id_is_explicit_label_sum_bpm_per_duration
    FROM tagged
    WINDOW w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date DESC, id)
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, a.debut_year AS artist_debut_year, a.genre AS artist_genre
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date
    FROM joined s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.is_explicit_label,
        s.title_clean,
        s.is_explicit_flag,
        s.bpm_per_duration_sec,
        s.duration_sec_filled_change,
        s.duration_sec_filled_rank,
        s.second_duration_sec,
        s.artist_id_is_explicit_label_sum_bpm_per_duration,
        s.artist_debut_year,
        s.artist_genre,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM summarized_rows s
    LEFT JOIN plays p ON p.track_id = s.id AND p.listener_id IS NOT NULL
)
SELECT id, play_id, metric, amount
FROM (
    SELECT id, play_id, artist_id_is_explicit_label_sum_bpm_per_duration, bpm_per_duration_sec
    FROM exploded_detail
) src
UNPIVOT (
    amount FOR metric IN (artist_id_is_explicit_label_sum_bpm_per_duration AS m_artist_id_is_explicit_label_sum_bpm_per_durati, bpm_per_duration_sec AS bpm_per_duration_sec_v)
) unp
ORDER BY id, play_id, metric
