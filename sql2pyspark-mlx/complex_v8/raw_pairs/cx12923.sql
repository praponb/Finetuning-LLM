WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 239) AS duration_sec_distance,
        ABS(bpm - 142) AS bpm_distance,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE (release_date >= DATE '2024-10-23' OR artist_id IS NOT NULL)
),
stats AS (
    -- Summarise per group
    SELECT
        is_explicit,
        SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) AS n_title_is_not_null,
        COUNT(*) AS cnt
    FROM cleaned
    WHERE ARRAY_CONTAINS(moods, 'chill')
    GROUP BY 1
    HAVING AVG(bpm_distance) > 36
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(n_title_is_not_null) OVER w_cnt AS cnt_max_n_title_is_not_null,
        NTILE(4) OVER (ORDER BY cnt, is_explicit) AS cnt_quartile
    FROM stats
    WINDOW w_cnt AS (PARTITION BY cnt)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY n_title_is_not_null, cnt) AS rows_in_n_title_is_not_null_cnt,
        NTH_VALUE(cnt_quartile, 2) OVER (PARTITION BY cnt ORDER BY cnt DESC, is_explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_cnt_quartile
    FROM sequenced
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ABS(n_title_is_not_null - 6) AS n_title_is_not_null_distance
    FROM with_windows
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN cnt_quartile >= 7 THEN 'HIGH' WHEN cnt_quartile >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_quartile_tier,
        CASE WHEN is_explicit_flag >= 1 THEN 'A' WHEN is_explicit_flag >= 0 THEN 'B' ELSE 'C' END AS is_explicit_flag_tier
    FROM enriched
    WHERE cnt_quartile BETWEEN 3 AND 10
)
SELECT
    is_explicit,
    n_title_is_not_null,
    cnt,
    cnt_max_n_title_is_not_null,
    cnt_quartile,
    rows_in_n_title_is_not_null_cnt,
    second_cnt_quartile,
    is_explicit_flag,
    n_title_is_not_null_distance,
    is_explicit_flag_tier
FROM annotated
ORDER BY cnt_quartile DESC, is_explicit
LIMIT 5
