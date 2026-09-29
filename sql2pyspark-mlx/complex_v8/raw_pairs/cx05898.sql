WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        YEAR(played_at) AS played_at_year,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        CONCAT_WS(' / ', source, device) AS source_device_label
    FROM plays
    WHERE track_id >= 9
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ms_played_bucket - LAG(ms_played_bucket) OVER w_all_by_played_at_year AS ms_played_bucket_change,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER (PARTITION BY is_skipped), 0), 4) AS ms_played_bucket_share_of_is_skipped,
        MIN(played_at_year) OVER w_is_skipped AS is_skipped_min_played_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY played_at_year ORDER BY played_at, id), 3) AS played_at_cume_dist
    FROM cleaned_plays
    WINDOW w_all_by_played_at_year AS (ORDER BY played_at_year DESC NULLS FIRST, id), w_is_skipped AS (PARTITION BY is_skipped)
),
labeled AS (
    -- Label and score rows
    SELECT *, CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM metrics
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.ms_played_bucket,
        s.ms_played_bucket_change,
        s.ms_played_bucket_share_of_is_skipped,
        s.is_skipped_min_played_at_year,
        s.played_at_cume_dist,
        s.is_skipped_flag,
        t.title AS track_title,
        t.is_explicit AS track_is_explicit,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM labeled s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
    WHERE NOT t.is_explicit
),
unique_rows AS (
    -- Keep the latest row per track_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        ms_played_bucket,
        ms_played_bucket_change,
        ms_played_bucket_share_of_is_skipped,
        is_skipped_min_played_at_year,
        played_at_cume_dist,
        is_skipped_flag,
        track_title,
        track_is_explicit,
        listener_plan,
        listener_username
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY is_skipped_min_played_at_year DESC, id DESC) AS row_num
        FROM enriched_track
    ) ranked
    WHERE row_num = 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.ms_played_bucket,
        s.ms_played_bucket_change,
        s.played_at_cume_dist,
        s.is_skipped_flag,
        s.track_title,
        s.track_is_explicit,
        s.listener_plan,
        s.listener_username,
        t.is_explicit AS track_is_explicit_2,
        t.artist_id AS track_artist_id,
        t.moods AS track_moods
    FROM unique_rows s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    track_artist_id,
    listener_id,
    SUM(CASE WHEN track_is_explicit_2 = TRUE THEN 1 ELSE 0 END) AS n_track_is_explicit_2_true,
    MIN(played_at) AS first_played_at
FROM with_details
GROUP BY CUBE(track_artist_id, listener_id)
ORDER BY track_artist_id NULLS LAST, listener_id NULLS LAST
