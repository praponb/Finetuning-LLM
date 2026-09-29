WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.debut_year <= 2006
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        release_date,
        is_explicit,
        NTH_VALUE(release_date, 2) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        RANK() OVER (PARTITION BY is_explicit ORDER BY release_date) AS release_date_rank,
        LEAD(release_date) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY release_date, id) AS next_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date), 3) AS release_date_percent_rank,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id)) AS days_since_prev_release_date,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC, id) AS release_date_quintile,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date, id) AS release_date_dense_rank,
        FIRST_VALUE(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        LAG(release_date, 3) OVER (ORDER BY release_date DESC, id) AS prev3_release_date,
        ROUND(CUME_DIST() OVER w_release_date_month_by_releas, 3) AS release_date_cume_dist
    FROM with_details
    WINDOW w_release_date_month_by_releas AS (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        release_date,
        is_explicit,
        second_release_date,
        next_release_date,
        release_date_percent_rank,
        days_since_prev_release_date,
        release_date_quintile,
        release_date_dense_rank,
        first_release_date,
        prev3_release_date,
        release_date_cume_dist
    FROM with_windows
    WHERE release_date_rank <= 3
      AND second_release_date IS NOT NULL
)
-- Final projection
SELECT
    id,
    release_date,
    is_explicit,
    second_release_date,
    release_date_percent_rank,
    days_since_prev_release_date,
    release_date_quintile,
    release_date_dense_rank,
    first_release_date,
    prev3_release_date
FROM leaders
ORDER BY release_date_dense_rank DESC
