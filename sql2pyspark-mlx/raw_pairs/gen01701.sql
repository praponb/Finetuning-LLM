WITH kept AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost IS NOT NULL
        )
),
scored AS (
    SELECT
        *,
        COUNT(rating) OVER w_ordered AS running_count_rating,
        AVG(rating) OVER w_ordered AS running_avg_rating,
        SUM(rating) OVER w_ordered AS running_sum_rating,
        NTILE(4) OVER (ORDER BY rating ASC, id ASC) AS rating_bucket
    FROM kept
    WHERE LENGTH(supplier_name) >= 14
    WINDOW w_ordered AS (PARTITION BY city, country ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        LEAD(running_sum_rating, 2, 0) OVER (PARTITION BY country ORDER BY rating ASC, id ASC) AS next_running_sum_rating,
        AVG(rating) OVER (PARTITION BY city ORDER BY running_avg_rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        FIRST_VALUE(running_avg_rating) IGNORE NULLS OVER (PARTITION BY city ORDER BY running_avg_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_rating,
        CUME_DIST() OVER (PARTITION BY city ORDER BY running_count_rating DESC) AS running_count_rating_cume_dist
    FROM scored
),
with_latest AS (
    SELECT
        s.id,
        s.supplier_name,
        s.running_count_rating,
        s.running_sum_rating,
        s.first_known_running_avg_rating,
        latest.brand
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    brand,
    AVG(first_known_running_avg_rating) FILTER (WHERE brand = 'Initech') AS initech_avg_first_known_running_avg_rating,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SUM(CASE WHEN brand = 'Initech' THEN first_known_running_avg_rating ELSE 0 END) AS initech_first_known_running_avg_rating,
    COUNT(DISTINCT brand) AS distinct_brands,
    ROUND(AVG(running_sum_rating), 2) AS avg_running_sum_rating
FROM with_latest
GROUP BY 1
HAVING SUM(CASE WHEN brand = 'Initech' THEN first_known_running_avg_rating ELSE 0 END) > 0;
