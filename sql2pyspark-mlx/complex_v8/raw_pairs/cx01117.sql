WITH scoped_tracks AS (
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
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
      AND bpm > 89
),
per_group AS (
    -- Summarise per group
    SELECT
        title_len,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(title_len) FILTER (WHERE UPPER(TRIM(title)) LIKE 'G%') AS title_len_upper_trim_title_like_g
    FROM scoped_tracks
    GROUP BY title_len
    HAVING COUNT(*) >= 2
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE title_len_upper_trim_title_like_g > (SELECT MIN(title_len_upper_trim_title_like_g) FROM per_group)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(distinct_ids / NULLIF(title_len, 0), 3) AS distinct_ids_per_title_len,
        CASE WHEN title_len >= 11 THEN 'large' WHEN title_len >= 7 THEN 'medium' ELSE 'small' END AS title_len_tier,
        title_len + distinct_ids AS title_len_plus_distinct_ids
    FROM filtered
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        title_len,
        n_rows,
        title_len_upper_trim_title_like_g,
        title_len_tier,
        title_len_plus_distinct_ids,
        ROW_NUMBER() OVER (ORDER BY n_rows DESC, title_len) AS rn,
        COUNT(*) OVER w_title_len_tier AS rows_in_title_len_tier
    FROM annotated
    WINDOW w_title_len_tier AS (PARTITION BY title_len_tier)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE title_len > (SELECT AVG(title_len) FROM sequenced)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(n_rows - AVG(n_rows) OVER w_rn_n_rows, 2) AS n_rows_vs_rn_n_rows_avg,
        MIN(rows_in_title_len_tier) OVER (PARTITION BY rn, title_len ORDER BY n_rows DESC, title_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rows_in_title_len_tier_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows ORDER BY rows_in_title_len_tier ASC, title_len), 3) AS rows_in_title_len_tier_cume_dist,
        DENSE_RANK() OVER (PARTITION BY title_len ORDER BY rows_in_title_len_tier DESC) AS rows_in_title_len_tier_dense_rank,
        COUNT(*) OVER w_rn_n_rows AS rows_in_rn_n_rows,
        LAG(title_len, 3) OVER (PARTITION BY rn, title_len ORDER BY n_rows DESC, title_len) AS prev3_title_len
    FROM qualified
    WINDOW w_rn_n_rows AS (PARTITION BY rn, n_rows)
)
-- Final projection
SELECT
    title_len,
    title_len_upper_trim_title_like_g,
    title_len_tier,
    title_len_plus_distinct_ids,
    rn,
    rows_in_title_len_tier,
    min_rows_in_title_len_tier_so_far,
    rows_in_title_len_tier_dense_rank,
    rows_in_rn_n_rows,
    prev3_title_len
FROM with_windows
ORDER BY rn, rows_in_title_len_tier DESC
