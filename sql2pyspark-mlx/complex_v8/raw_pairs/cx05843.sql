WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE bpm BETWEEN 94 AND 156
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        release_date,
        moods,
        release_date_month,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        ROUND((UNIX_TIMESTAMP(release_date_month) - UNIX_TIMESTAMP(LAG(release_date_month) OVER w_all_by_duration_sec)) / 60, 1) AS minutes_since_prev_release_date_month
    FROM prepared_tracks
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec ASC, id)
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_windows s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
        )
),
latest AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        moods,
        release_date_month,
        duration_sec_avg_4,
        duration_sec_vs_artist_id_avg,
        duration_sec_share_of_artist_id,
        minutes_since_prev_release_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_month, id) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(duration_sec_avg_4) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec_avg_4,
        ROUND(AVG(duration_sec_avg_4) OVER (PARTITION BY artist_id ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4_avg_4,
        ROUND(AVG(duration_sec_avg_4) OVER (ORDER BY duration_sec_vs_artist_id_avg NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4_avg_7,
        ROW_NUMBER() OVER w_all_by_duration_sec_vs_artis_2 AS rn
    FROM latest
    WINDOW w_all_by_duration_sec_vs_artis_2 AS (ORDER BY duration_sec_vs_artist_id_avg NULLS LAST, id)
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        moods,
        release_date_month,
        duration_sec_avg_4,
        artist_id_sum_duration_sec_avg_4,
        duration_sec_avg_4_avg_4,
        SIZE(COLLECT_SET(artist_id) OVER w_rn_release_date_year) AS distinct_artist_id_in_rn_release_date_year,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec_share_of_artist_id DESC NULLS FIRST), 3) AS duration_sec_share_of_artist_id_cume_dist
    FROM ranked
    WINDOW w_rn_release_date_year AS (PARTITION BY rn, YEAR(release_date))
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec_avg_4,
        s.duration_sec_avg_4_avg_4,
        s.distinct_artist_id_in_rn_release_date_year,
        s.duration_sec_share_of_artist_id_cume_dist,
        a.debut_year AS artist_debut_year
    FROM ranked_2 s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year < 2020
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, artist_debut_year, distinct_artist_id_in_rn_release_date_year
    FROM joined
) src
UNPIVOT (
    metric_value FOR field IN (artist_debut_year AS artist_debut_year_v, distinct_artist_id_in_rn_release_date_year AS m_distinct_artist_id_in_rn_release_date_year)
) unp
ORDER BY id, field
