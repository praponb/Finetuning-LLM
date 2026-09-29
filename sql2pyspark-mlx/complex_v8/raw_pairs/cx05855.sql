WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-24 11:00:00'
      AND ms_played <= 333795
),
deduped AS (
    -- Keep the latest row per source
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_filled,
        device_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY ms_played_filled DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(ms_played_filled, 2, 0) OVER (PARTITION BY listener_id, device ORDER BY ms_played DESC, id) AS prev2_ms_played_filled,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER w_track_id_device, 0), 4) AS ms_played_filled_share_of_track_id_device
    FROM deduped
    WINDOW w_track_id_device AS (PARTITION BY track_id, device)
),
per_group AS (
    -- Summarise per group
    SELECT
        device,
        source,
        is_skipped,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE device IN ('speaker', 'desktop', 'phone')) AS n_device_in_speaker_desktop_ph
    FROM with_windows
    GROUP BY 1, 2, 3
    HAVING SUM(ms_played) > 107912
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM per_group)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(distinct_ids - 5) AS distinct_ids_distance,
        CASE WHEN source IN ('radio', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM without_matches
    WHERE n_device_in_speaker_desktop_ph < 9
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        device,
        source,
        is_skipped,
        n_device_in_speaker_desktop_ph,
        distinct_ids_distance,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids,
        ROUND(distinct_ids - AVG(distinct_ids) OVER w_distinct_ids, 2) AS distinct_ids_vs_distinct_ids_avg
    FROM annotated
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
SELECT
    device,
    is_skipped,
    n_device_in_speaker_desktop_ph,
    distinct_ids_distance,
    rows_in_distinct_ids,
    distinct_ids_vs_distinct_ids_avg,
    LEAST(rows_in_distinct_ids, distinct_ids_distance) AS least_rows_in_distinct_ids_distinct_ids_distance
FROM with_windows_2
ORDER BY distinct_ids_distance NULLS LAST, least_rows_in_distinct_ids_distinct_ids_distance
