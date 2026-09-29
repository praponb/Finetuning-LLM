WITH scored AS (
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share,
        MAX(rating) OVER (PARTITION BY country) AS country_max_rating,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating NULLS FIRST) AS rating_percent_rank
    FROM suppliers
),
scored_suppliers AS (
    SELECT
        *,
        SUM(rating_avg_2) OVER (ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_avg_2,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating_share DESC NULLS FIRST) AS rating_share_dense_rank
    FROM scored
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_share DESC, id) AS row_num
        FROM scored_suppliers
    ) ranked
    WHERE row_num = 1
),
scored_suppliers_2 AS (
    SELECT
        *,
        LEAD(country_max_rating, 2) OVER (PARTITION BY country ORDER BY rating_share_dense_rank, id) AS next_country_max_rating,
        SUM(country_max_rating) OVER (PARTITION BY city, country) AS city_sum_country_max_rating
    FROM latest
    WHERE country_max_rating <= 2
),
eligible AS (
    SELECT s.*
    FROM scored_suppliers_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty > 162
        )
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY country_max_rating DESC, id) AS rank_in_group
    FROM eligible
    QUALIFY rank_in_group = 1
),
latest_suppliers AS (
    SELECT
        id,
        city,
        rating_share_dense_rank,
        row_num,
        city_sum_country_max_rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY city_sum_country_max_rating DESC, id) AS rank_in_group_2
        FROM deduped
    ) x
    WHERE rank_in_group_2 = 1
)
SELECT
    id,
    city,
    city_sum_country_max_rating
FROM latest_suppliers
ORDER BY city_sum_country_max_rating
