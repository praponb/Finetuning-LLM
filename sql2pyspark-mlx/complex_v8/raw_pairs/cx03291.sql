WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
),
deduped AS (
    -- Keep the latest row per is_explicit_label
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        is_explicit_label,
        title_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY is_explicit_label ORDER BY bpm DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM deduped
    WHERE artist_id >= 11
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(bpm, 0) AS bpm_filled,
        COALESCE(duration_sec, 0) AS duration_sec_filled_2
    FROM tagged
)
SELECT
    artist_id,
    is_explicit_label,
    GROUPING_ID() AS gid,
    MIN(bpm) AS min_bpm,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list
FROM annotated
GROUP BY GROUPING SETS ((artist_id, is_explicit_label), (artist_id))
ORDER BY artist_id NULLS LAST, is_explicit_label NULLS LAST
