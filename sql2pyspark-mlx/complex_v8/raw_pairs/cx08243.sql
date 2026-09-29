WITH filtered AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        is_verified,
        UPPER(TRIM(genre)) AS genre_clean,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
    FROM artists
    WHERE genre IN ('Pop', 'Jazz', 'Indie')
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        artist_name,
        is_verified,
        genre_clean,
        country_slug,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM filtered
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        is_verified,
        country_slug,
        genre_or_unknown,
        is_verified_flag,
        SUM(is_verified_flag) OVER (PARTITION BY is_verified_flag, is_verified ORDER BY is_verified_flag NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS is_verified_flag_sum_2_prior,
        LEAD(is_verified_flag) OVER (ORDER BY is_verified_flag DESC, id) AS next_is_verified_flag,
        ROUND(is_verified_flag / NULLIF(SUM(is_verified_flag) OVER (PARTITION BY is_verified_flag, genre_or_unknown), 0), 4) AS is_verified_flag_share_of_is_verified_flag_genre,
        LAG(is_verified_flag, 1, 0) OVER (ORDER BY is_verified_flag DESC, id) AS prev_is_verified_flag,
        ROW_NUMBER() OVER (ORDER BY is_verified_flag DESC, id) AS seq_no,
        ROUND(AVG(is_verified_flag) OVER (ORDER BY is_verified_flag DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS is_verified_flag_avg_1_prior
    FROM annotated
),
stats AS (
    -- Group-level totals
    SELECT
        is_verified,
        seq_no,
        ROUND(AVG(is_verified_flag), 2) AS avg_is_verified_flag,
        COUNT(*) AS n_rows
    FROM windowed
    WHERE seq_no > 5
    GROUP BY is_verified, seq_no
    HAVING MAX(is_verified_flag_sum_2_prior) > 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_n_rows AS rows_in_n_rows,
        SUM(n_rows) OVER (PARTITION BY n_rows) AS n_rows_sum_n_rows,
        ROUND(CUME_DIST() OVER (PARTITION BY is_verified, n_rows ORDER BY avg_is_verified_flag ASC), 3) AS avg_is_verified_flag_cume_dist
    FROM stats
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
-- Final projection
SELECT
    is_verified,
    seq_no,
    n_rows,
    rows_in_n_rows,
    n_rows_sum_n_rows,
    avg_is_verified_flag_cume_dist
FROM ranked
ORDER BY rows_in_n_rows DESC
