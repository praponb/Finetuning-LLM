WITH prepared_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        country,
        signup_date,
        birth_year,
        interests,
        CONCAT(username, '#', CAST(id AS STRING)) AS username_ref,
        INITCAP(TRIM(username)) AS username_clean
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'indie')
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_ym,
        COUNT(*) AS row_count,
        SUM(birth_year) AS total_birth_year,
        COUNT(*) FILTER (WHERE country IS NOT NULL) AS n_country_is_not_null,
        SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) AS n_country_is_not_null_2
    FROM prepared_listeners
    GROUP BY 1
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE n_country_is_not_null < (SELECT MAX(n_country_is_not_null) FROM grouped)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        signup_date_ym,
        row_count,
        n_country_is_not_null,
        n_country_is_not_null_2,
        ROUND(CUME_DIST() OVER (PARTITION BY row_count, n_country_is_not_null_2 ORDER BY total_birth_year DESC), 3) AS total_birth_year_cume_dist,
        MAX(n_country_is_not_null) OVER (PARTITION BY row_count, n_country_is_not_null_2 ORDER BY total_birth_year DESC, signup_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_country_is_not_null_so_far,
        LEAD(row_count, 1, 0) OVER w_row_count_n_country_is_not_n_3 AS next_row_count,
        DENSE_RANK() OVER (PARTITION BY row_count, n_country_is_not_null_2 ORDER BY total_birth_year DESC) AS total_birth_year_dense_rank,
        NTILE(4) OVER (PARTITION BY row_count, n_country_is_not_null_2 ORDER BY total_birth_year DESC, signup_date_ym) AS total_birth_year_quartile,
        MAX(n_country_is_not_null_2) OVER (PARTITION BY row_count, n_country_is_not_null_2 ORDER BY total_birth_year DESC, signup_date_ym ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n_country_is_not_null_2_max_2
    FROM with_activity
    WINDOW w_row_count_n_country_is_not_n_3 AS (PARTITION BY row_count, n_country_is_not_null_2 ORDER BY total_birth_year DESC, signup_date_ym)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE total_birth_year_dense_rank <= 5
)
SELECT
    signup_date_ym,
    row_count,
    n_country_is_not_null,
    n_country_is_not_null_2,
    max_n_country_is_not_null_so_far,
    total_birth_year_dense_rank,
    total_birth_year_quartile,
    n_country_is_not_null_2_max_2,
    CONCAT(signup_date_ym, '#', CAST(next_row_count AS STRING)) AS signup_date_ym_ref
FROM first_rows
ORDER BY max_n_country_is_not_null_so_far DESC
