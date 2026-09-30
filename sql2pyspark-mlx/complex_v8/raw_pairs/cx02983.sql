WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        days_since_release_date,
        title_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS rn
        FROM filtered_tracks
    ) ranked
    WHERE rn = 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.moods,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM latest s
    JOIN artists a ON a.id = s.artist_id
),
per_group AS (
    -- Summarise per group
    SELECT
        artist_id,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) AS cnt,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
        COUNT(DISTINCT artist_country) AS distinct_artist_countrys
    FROM with_details
    WHERE artist_genre IS NOT NULL
    GROUP BY 1, 2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        artist_id,
        release_date_month,
        cnt,
        release_date_span_days,
        distinct_artist_countrys,
        ROUND(cnt - AVG(cnt) OVER (PARTITION BY cnt, distinct_artist_countrys), 2) AS cnt_vs_cnt_distinct_artist_countrys_avg,
        ROUND(release_date_span_days / NULLIF(SUM(release_date_span_days) OVER (PARTITION BY cnt, distinct_artist_countrys), 0), 4) AS release_date_span_days_share_of_cnt_distinct_art,
        LAST_VALUE(release_date_span_days) IGNORE NULLS OVER (PARTITION BY distinct_artist_countrys ORDER BY distinct_artist_countrys NULLS LAST, artist_id, release_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_span_days,
        FIRST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY distinct_artist_countrys ORDER BY distinct_artist_countrys NULLS LAST, artist_id, release_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cnt,
        LAG(distinct_artist_countrys, 1, 0) OVER (ORDER BY cnt DESC NULLS FIRST, artist_id, release_date_month) AS prev_distinct_artist_countrys
    FROM per_group
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.debut_year AS artist_debut_year
    FROM with_windows s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    artist_id,
    release_date_month,
    cnt,
    release_date_span_days,
    distinct_artist_countrys,
    cnt_vs_cnt_distinct_artist_countrys_avg,
    release_date_span_days_share_of_cnt_distinct_art,
    last_known_release_date_span_days,
    first_known_cnt,
    prev_distinct_artist_countrys,
    artist_debut_year
FROM enriched_artist
ORDER BY last_known_release_date_span_days, artist_id, release_date_month
LIMIT 20
