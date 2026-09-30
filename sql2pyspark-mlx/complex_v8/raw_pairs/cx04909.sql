WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        SIZE(moods) AS moods_count,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
    WHERE duration_sec <= 392
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods_count,
        s.release_date_week,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        release_date_week,
        artist_is_verified,
        artist_debut_year,
        artist_country,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROUND(moods_count / NULLIF(SUM(moods_count) OVER w_artist_id, 0), 4) AS moods_count_share_of_artist_id,
        bpm - LAG(bpm) OVER (PARTITION BY moods_count ORDER BY artist_debut_year, id) AS bpm_change,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY moods_count ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id
    FROM enriched_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    release_date,
    release_date_week,
    artist_is_verified,
    artist_debut_year,
    artist_id_max_bpm,
    moods_count_share_of_artist_id,
    bpm_change,
    second_release_date,
    DATE_TRUNC('QUARTER', release_date_week) AS release_date_week_quarter,
    bpm_change + artist_id_max_bpm AS bpm_change_plus_artist_id_max_bpm
FROM metrics
