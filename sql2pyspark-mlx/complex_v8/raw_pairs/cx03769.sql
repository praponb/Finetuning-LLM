WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        TRUNC(release_date, 'MM') AS release_date_month_start
    FROM tracks
    WHERE release_date < DATE '2025-03-08'
      AND bpm > 75
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date_month_start) AS release_date_month_start_month,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE UPPER(TRIM(title)) LIKE 'M%') AS n_upper_trim_title_like_m,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(title)) >= 8 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_title_8
    FROM scoped_tracks
    GROUP BY 1
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN distinct_artist_ids >= 8 THEN 'HIGH' WHEN distinct_artist_ids >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_artist_ids_tier,
        cnt - pct_length_trim_title_8 AS cnt_minus_pct_length_trim_title_8
    FROM rollup_base
)
SELECT
    release_date_month_start_month,
    distinct_artist_ids,
    cnt,
    pct_length_trim_title_8,
    distinct_artist_ids_tier,
    cnt_minus_pct_length_trim_title_8
FROM enriched
WHERE pct_length_trim_title_8 >= 45.0
ORDER BY cnt DESC NULLS FIRST, distinct_artist_ids_tier, release_date_month_start_month
LIMIT 10
