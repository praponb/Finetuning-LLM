WITH base AS (
    SELECT
        *,
        CONCAT_WS('-', supplier_name, country) AS supplier_name_country_key,
        LOWER(country) AS country_lower,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM suppliers
    WHERE city = 'Singapore'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST, id) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY dedup_rank DESC, id) AS row_num
    FROM latest
    QUALIFY row_num = 1
),
kept_keys AS (
    SELECT country, city
    FROM ranked
    INTERSECT
    SELECT country, city
    FROM suppliers
    WHERE city IN ('London', 'New York')
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.country = k.country AND s.city = k.city
)
SELECT
    supplier_name,
    rating,
    country_lower,
    city_clean,
    dedup_rank,
    row_num,
    PERCENT_RANK() OVER (ORDER BY rating DESC) AS rating_percent_rank,
    DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank
FROM reconciled;
