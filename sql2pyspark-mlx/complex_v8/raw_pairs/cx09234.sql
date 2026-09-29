WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE SIZE(moods) >= 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.release_date_label,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        release_date,
        moods,
        release_date_label,
        artist_country,
        artist_debut_year,
        ROW_NUMBER() OVER (PARTITION BY artist_is_verified ORDER BY release_date DESC NULLS FIRST, id) AS release_date_seq,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_artist_debut_year,
        COUNT(*) OVER (PARTITION BY artist_country) AS rows_in_artist_country,
        FIRST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_country ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_debut_year,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_is_verified ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        COUNT(*) OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER w_artist_country_by_release_da_2 AS seq_no,
        RANK() OVER (PARTITION BY artist_country ORDER BY release_date ASC, id) AS release_date_rank,
        COUNT(*) OVER (PARTITION BY artist_country ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2
    FROM with_details
    WINDOW w_artist_country_by_release_da_2 AS (PARTITION BY artist_country ORDER BY release_date ASC, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    moods,
    release_date_label,
    artist_country,
    release_date_seq,
    artist_country_avg_artist_debut_year,
    rows_in_artist_country,
    first_known_artist_debut_year,
    last_release_date,
    running_count,
    seq_no,
    release_date_rank,
    running_count_2,
    LEAST(running_count, release_date_rank) AS least_running_count_release_date_rank,
    release_date_seq + seq_no AS release_date_seq_plus_seq_no
FROM windowed
WHERE release_date_label IS NOT NULL
