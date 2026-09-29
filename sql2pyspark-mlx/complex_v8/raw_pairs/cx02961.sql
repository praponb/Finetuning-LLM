WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        is_verified
    FROM artists
    WHERE debut_year < 2015
      AND country <> 'BR'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(artist_name), 1, 4) AS artist_name_prefix,
        INITCAP(TRIM(artist_name)) AS artist_name_clean,
        CONCAT(artist_name, '#', CAST(id AS STRING)) AS artist_name_ref
    FROM prepared
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.title IS NOT NULL
        )
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.is_explicit = TRUE
              AND t.bpm BETWEEN 76 AND 156
        )
),
qualified_2 AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND SIZE(t.moods) >= 2
              AND t.duration_sec < 402
        )
      AND UPPER(TRIM(s.artist_name)) LIKE 'S%'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        country,
        COUNT(*) FILTER (WHERE country <> 'KR') AS n_country_kr,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN country NOT IN ('TH', 'US') THEN 1 ELSE 0 END) AS n_country_not_in_th_us,
        SUM(CASE WHEN country NOT IN ('US', 'KR') THEN 1 ELSE 0 END) AS n_country_not_in_us_kr
    FROM qualified_2
    GROUP BY 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        country,
        n_country_kr,
        n_rows,
        distinct_ids,
        n_country_not_in_th_us,
        n_country_not_in_us_kr,
        LAST_VALUE(distinct_ids) OVER (PARTITION BY n_country_kr, distinct_ids ORDER BY n_country_not_in_us_kr ASC, country ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_ids,
        NTILE(5) OVER (PARTITION BY n_country_kr, distinct_ids ORDER BY n_country_not_in_us_kr ASC, country) AS n_country_not_in_us_kr_quintile,
        ROUND(PERCENT_RANK() OVER w_n_country_kr_distinct_ids_by_3, 3) AS n_country_not_in_us_kr_percent_rank,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY n_country_kr, distinct_ids ORDER BY n_country_not_in_us_kr ASC, country ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        LEAD(n_country_not_in_th_us) OVER (PARTITION BY n_country_kr, distinct_ids ORDER BY n_country_not_in_us_kr ASC, country) AS next_n_country_not_in_th_us,
        ROUND(CUME_DIST() OVER w_n_country_kr_distinct_ids_by_2, 3) AS n_country_not_in_us_kr_cume_dist
    FROM summary
    WINDOW w_n_country_kr_distinct_ids_by_3 AS (PARTITION BY n_country_kr, distinct_ids ORDER BY n_country_not_in_us_kr ASC), w_n_country_kr_distinct_ids_by_2 AS (PARTITION BY n_country_kr, distinct_ids ORDER BY n_country_not_in_us_kr ASC, country)
)
-- Native SQL UNPIVOT (wide to long)
SELECT country, metric_name, amount
FROM (
    SELECT country, n_country_not_in_us_kr_cume_dist, n_country_not_in_us_kr_percent_rank
    FROM windowed
) src
UNPIVOT (
    amount FOR metric_name IN (n_country_not_in_us_kr_cume_dist, n_country_not_in_us_kr_percent_rank)
) unp
