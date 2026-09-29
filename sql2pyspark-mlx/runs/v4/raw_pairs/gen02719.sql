WITH scored AS (
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        ROW_NUMBER() OVER w_recent AS row_seq,
        ROW_NUMBER() OVER w_recent AS event_seq,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_dense_rank,
        LAG(rating, 2) OVER w_grp AS prev_rating
    FROM suppliers
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating DESC, id), w_grp AS (PARTITION BY city, country ORDER BY rating, id)
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating_avg_2,
    rating_dense_rank,
    prev_rating,
    CASE WHEN supplier_name RLIKE '^(Global Supply|Pacific Wholesale)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS supplier_name_zone
FROM scored
ORDER BY prev_rating DESC
