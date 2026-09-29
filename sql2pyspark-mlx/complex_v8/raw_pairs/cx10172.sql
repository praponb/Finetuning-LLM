WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        CASE WHEN bpm >= 156 THEN 'HIGH' WHEN bpm >= 89 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND duration_sec > 144
),
stats AS (
    -- Group-level totals
    SELECT
        bpm_tier,
        is_explicit,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM recent
    GROUP BY bpm_tier, is_explicit
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(distinct_ids, 0) AS distinct_ids_filled,
        GREATEST(distinct_ids, row_count) AS greatest_distinct_ids_row_count
    FROM stats
),
scored AS (
    -- Add calculated columns
    SELECT *, LEAST(row_count, distinct_ids) AS least_row_count_distinct_ids
    FROM enriched
)
SELECT
    bpm_tier,
    row_count,
    distinct_ids_filled,
    greatest_distinct_ids_row_count,
    least_row_count_distinct_ids,
    COALESCE(distinct_ids_filled, 0) AS distinct_ids_filled_filled
FROM scored
ORDER BY row_count
