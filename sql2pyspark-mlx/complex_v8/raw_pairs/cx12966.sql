WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ABS(ms_played - 195566) AS ms_played_distance,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE source = 'search'
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT COUNT(*) FILTER (WHERE p.device IS NOT NULL) FROM plays p WHERE p.track_id = s.track_id) AS play_device_count
    FROM filtered s
),
latest AS (
    -- Keep the latest row per device
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ms_played_distance,
        source_ref,
        played_at_date,
        play_count,
        play_device_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device ORDER BY play_device_count DESC, id DESC) AS recency_rank
        FROM summarized_rows
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(played_at) ORDER BY played_at), 3) AS played_at_cume_dist,
        LEAD(played_at_date) OVER w_played_at_year_by_played_at_2 AS next_played_at_date,
        ROUND(play_count / NULLIF(SUM(play_count) OVER (PARTITION BY MONTH(played_at), play_device_count), 0), 4) AS play_count_share_of_played_at_month_play_device_
    FROM latest
    WINDOW w_played_at_year_by_played_at_2 AS (PARTITION BY YEAR(played_at) ORDER BY played_at, id)
),
unique_rows AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ms_played_distance,
        source_ref,
        played_at_date,
        play_count,
        play_device_count,
        played_at_cume_dist,
        next_played_at_date,
        play_count_share_of_played_at_month_play_device_
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY next_played_at_date DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ms_played_distance,
        source_ref,
        played_at_date,
        play_count,
        play_device_count,
        played_at_cume_dist,
        DATE_TRUNC('QUARTER', played_at_date) AS played_at_date_quarter
    FROM unique_rows
    WHERE source_ref IS NOT NULL
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    played_at_date,
    play_device_count,
    played_at_date_quarter
FROM scored
ORDER BY ms_played DESC, played_at_date NULLS LAST
