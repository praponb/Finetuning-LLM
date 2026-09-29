WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE duration_sec < 357
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM cleaned_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source NOT IN ('search', 'playlist')
        )
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true,
        SUM(CASE WHEN NOT is_explicit THEN days_since_release_date ELSE 0 END) AS days_since_release_date_not_is_explicit
    FROM screened
    GROUP BY 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        release_date_month,
        n_is_explicit_true,
        days_since_release_date_not_is_explicit,
        ROUND((UNIX_TIMESTAMP(release_date_month) - UNIX_TIMESTAMP(LAG(release_date_month) OVER (PARTITION BY n_is_explicit_true, DATE_TRUNC('MONTH', release_date_month) ORDER BY n_is_explicit_true, release_date_month))) / 60, 1) AS minutes_since_prev_release_date_month,
        LEAD(release_date_month, 3) OVER (PARTITION BY n_is_explicit_true, DATE_TRUNC('MONTH', release_date_month) ORDER BY n_is_explicit_true, release_date_month) AS next3_release_date_month,
        ROUND(AVG(n_is_explicit_true) OVER (PARTITION BY n_is_explicit_true, DATE_TRUNC('MONTH', release_date_month) ORDER BY n_is_explicit_true, release_date_month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS n_is_explicit_true_avg_1_prior,
        LAG(release_date_month, 2) OVER (PARTITION BY n_is_explicit_true, DATE_TRUNC('MONTH', release_date_month) ORDER BY n_is_explicit_true, release_date_month) AS prev2_release_date_month,
        ROUND(n_is_explicit_true - AVG(n_is_explicit_true) OVER (PARTITION BY n_is_explicit_true), 2) AS n_is_explicit_true_vs_n_is_explicit_true_avg,
        ROUND(AVG(n_is_explicit_true) OVER (PARTITION BY n_is_explicit_true), 2) AS n_is_explicit_true_avg_n_is_explicit_true
    FROM summary
)
SELECT
    release_date_month,
    n_is_explicit_true,
    days_since_release_date_not_is_explicit,
    minutes_since_prev_release_date_month,
    next3_release_date_month,
    n_is_explicit_true_avg_1_prior,
    prev2_release_date_month,
    n_is_explicit_true_vs_n_is_explicit_true_avg,
    n_is_explicit_true_avg_n_is_explicit_true,
    DATE_FORMAT(release_date_month, 'yyyy-MM-dd HH:00') AS release_date_month_label
FROM with_trends
ORDER BY prev2_release_date_month DESC, n_is_explicit_true_vs_n_is_explicit_true_avg NULLS LAST, release_date_month
LIMIT 10
