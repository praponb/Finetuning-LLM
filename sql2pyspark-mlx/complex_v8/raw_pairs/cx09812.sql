WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        COALESCE(bpm, 0) AS bpm_filled,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE title IS NOT NULL
      AND bpm <= 166
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        MAX(release_date) AS last_release_date,
        MAX(bpm_filled) AS max_bpm_filled,
        COUNT(*) FILTER (WHERE NOT is_explicit) AS n_not_is_explicit
    FROM valid
    GROUP BY DATE_TRUNC('MONTH', release_date)
    HAVING SUM(bpm) > 82
),
scored AS (
    -- Label and score rows
    SELECT
        release_date_month,
        last_release_date,
        max_bpm_filled,
        n_not_is_explicit,
        max_bpm_filled + n_not_is_explicit AS max_bpm_filled_plus_n_not_is_explicit
    FROM grouped
    WHERE n_not_is_explicit < 6
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        release_date_month,
        last_release_date,
        max_bpm_filled,
        max_bpm_filled_plus_n_not_is_explicit,
        COUNT(*) OVER w_last_release_date_month_n_no AS rows_in_last_release_date_month_n_not_is_explici,
        LEAD(max_bpm_filled, 1, 0) OVER (PARTITION BY n_not_is_explicit ORDER BY last_release_date ASC, release_date_month) AS next_max_bpm_filled,
        NTILE(3) OVER w_n_not_is_explicit_by_last_re AS last_release_date_tercile,
        max_bpm_filled - LAG(max_bpm_filled) OVER (PARTITION BY n_not_is_explicit ORDER BY last_release_date ASC, release_date_month) AS max_bpm_filled_change,
        LAG(n_not_is_explicit, 2) OVER (PARTITION BY n_not_is_explicit ORDER BY last_release_date ASC, release_date_month) AS prev2_n_not_is_explicit
    FROM scored
    WINDOW w_last_release_date_month_n_no AS (PARTITION BY MONTH(last_release_date), n_not_is_explicit), w_n_not_is_explicit_by_last_re AS (PARTITION BY n_not_is_explicit ORDER BY last_release_date ASC, release_date_month)
)
SELECT
    release_date_month,
    last_release_date,
    max_bpm_filled,
    max_bpm_filled_plus_n_not_is_explicit,
    next_max_bpm_filled,
    max_bpm_filled_change,
    prev2_n_not_is_explicit
FROM sequenced
ORDER BY max_bpm_filled_plus_n_not_is_explicit, prev2_n_not_is_explicit
