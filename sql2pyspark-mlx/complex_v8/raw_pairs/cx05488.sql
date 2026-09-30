WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        bpm - duration_sec AS bpm_minus_duration_sec,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix_2
    FROM tracks
    WHERE duration_sec > 165
      AND is_explicit = TRUE
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_id ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY release_date ASC), 3) AS release_date_percent_rank
    FROM prepared
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.title_prefix_2,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM with_trends s
    JOIN plays p ON p.track_id = s.id
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT_WS(': ', title, title_prefix_2) AS title_title_prefix_2_label
    FROM detail_rows_plays
),
deduped AS (
    -- Keep the latest row per play_source
    SELECT
        id,
        artist_id,
        title,
        release_date,
        title_prefix_2,
        play_id,
        play_device,
        play_played_at,
        play_source,
        title_title_prefix_2_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_source ORDER BY play_played_at DESC, id DESC, play_id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    title_prefix_2,
    play_device,
    play_played_at,
    play_source,
    title_title_prefix_2_label,
    COALESCE(play_device, 'UNKNOWN') AS play_device_or_unknown,
    CASE WHEN play_device IN ('phone', 'desktop') THEN 'group_a' WHEN play_device = 'car' THEN 'group_b' ELSE 'other' END AS play_device_group
FROM deduped
WHERE artist_id >= 9
