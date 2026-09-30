WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE LOWER(title) LIKE '%on%'
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM valid_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND LENGTH(TRIM(a.artist_name)) >= 4
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        ROUND(STDDEV(days_since_release_date), 2) AS stddev_days_since_release_date,
        MIN(days_since_release_date) AS min_days_since_release_date,
        COUNT(*) AS n_rows,
        MAX(duration_sec) AS max_duration_sec
    FROM filtered
    GROUP BY DATE_TRUNC('MONTH', release_date)
    HAVING AVG(duration_sec) > 183
),
exploded AS (
    -- One row per title_list_item
    SELECT
        s.release_date_month,
        s.stddev_days_since_release_date,
        s.min_days_since_release_date,
        s.n_rows,
        s.max_duration_sec,
        t.title_list_item_pos,
        t.title_list_item
    FROM rollup_base s
    LATERAL VIEW POSEXPLODE(s.title_list) t AS title_list_item_pos, title_list_item
),
derived AS (
    -- Derive helper columns
    SELECT *, release_date_month + INTERVAL 1 DAYS AS release_date_month_plus_1d
    FROM exploded
)
-- Final projection
SELECT
    release_date_month,
    min_days_since_release_date,
    n_rows,
    max_duration_sec,
    title_list_item_pos,
    title_list_item,
    release_date_month_plus_1d,
    n_rows + title_list_item_pos AS n_rows_plus_title_list_item_pos
FROM derived
ORDER BY release_date_month DESC, title_list_item NULLS LAST
