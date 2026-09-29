WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm
    FROM tracks
    WHERE title IS NOT NULL
      AND release_date < DATE '2025-08-07'
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (ORDER BY release_date ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_1_prior,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_release_date_month, 2) AS duration_sec_vs_release_date_month_avg
    FROM screened
    WINDOW w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date))
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.first_known_duration_sec,
        s.duration_sec_vs_release_date_month_avg,
        a.country AS artist_country,
        a.artist_name
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_artist_id_artist_country AS rows_in_artist_id_artist_country,
        ROUND(first_known_duration_sec - AVG(first_known_duration_sec) OVER w_artist_id_artist_country, 2) AS first_known_duration_sec_vs_artist_id_artist_cou,
        NTH_VALUE(first_known_duration_sec, 2) OVER (PARTITION BY artist_country ORDER BY duration_sec_vs_release_date_month_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_known_duration_sec,
        ROUND(first_known_duration_sec / NULLIF(SUM(first_known_duration_sec) OVER (PARTITION BY artist_id, artist_country), 0), 4) AS first_known_duration_sec_share_of_artist_id_arti,
        ROW_NUMBER() OVER (PARTITION BY artist_country ORDER BY duration_sec_vs_release_date_month_avg, id) AS seq_no
    FROM enriched_artist
    WINDOW w_artist_id_artist_country AS (PARTITION BY artist_id, artist_country)
)
SELECT
    id,
    artist_id,
    first_known_duration_sec,
    duration_sec_vs_release_date_month_avg,
    artist_country,
    rows_in_artist_id_artist_country,
    first_known_duration_sec_vs_artist_id_artist_cou,
    second_first_known_duration_sec,
    seq_no,
    FLOOR(first_known_duration_sec / 50) * 50 AS first_known_duration_sec_bucket
FROM with_windows
ORDER BY duration_sec_vs_release_date_month_avg DESC NULLS FIRST, first_known_duration_sec_vs_artist_id_artist_cou DESC
