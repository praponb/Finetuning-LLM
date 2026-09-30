WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE artist_id <= 9
      AND duration_sec <= 352
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ADD_MONTHS(release_date, 6) AS release_date_plus_6m,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM scoped_tracks
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(duration_sec_filled) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date_plus_6m NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_filled,
        ROUND(AVG(duration_sec_filled) OVER w_is_explicit, 2) AS is_explicit_avg_duration_sec_filled,
        MAX(duration_sec) OVER (PARTITION BY is_explicit ORDER BY release_date_plus_6m NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_max_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY release_date_plus_6m NULLS LAST), 3) AS release_date_plus_6m_cume_dist
    FROM scored
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
              AND p.ms_played <= 340114
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        release_date_plus_6m,
        duration_sec_filled,
        first_known_duration_sec_filled,
        is_explicit_avg_duration_sec_filled,
        duration_sec_max_2_prior,
        release_date_plus_6m_cume_dist,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM screened
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.first_known_duration_sec_filled,
        s.is_explicit_avg_duration_sec_filled,
        s.duration_sec_max_2_prior,
        s.release_date_plus_6m_cume_dist,
        s.is_explicit_label,
        a.debut_year AS artist_debut_year
    FROM tagged s
    JOIN artists a ON a.id = s.artist_id AND UPPER(TRIM(a.artist_name)) LIKE 'N%'
),
totals AS (
    -- Summarise per group
    SELECT
        is_explicit,
        MAX(release_date) AS last_release_date,
        ROUND(AVG(first_known_duration_sec_filled), 2) AS avg_first_known_duration_sec_filled,
        ROUND(SUM(artist_debut_year) / NULLIF(SUM(is_explicit_avg_duration_sec_filled), 0), 3) AS artist_debut_year_to_is_explicit_avg_duration_se,
        COUNT(*) FILTER (WHERE NOT is_explicit) AS n_not_is_explicit
    FROM enriched_artist
    GROUP BY is_explicit
)
-- Final projection
SELECT
    is_explicit,
    last_release_date,
    avg_first_known_duration_sec_filled,
    n_not_is_explicit,
    LAST_DAY(last_release_date) AS last_release_date_month_end,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM totals
ORDER BY last_release_date NULLS LAST
