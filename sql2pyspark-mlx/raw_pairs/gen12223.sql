WITH cleaned AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN city IN ('Singapore', 'Tokyo') THEN 'Group A' ELSE 'Other' END AS city_group
    FROM suppliers
    WHERE city IS NOT NULL
      AND city <> 'Bangkok'
),
enriched AS (
    SELECT
        *,
        AVG(rating) OVER w_ordered AS running_avg_rating,
        MAX(rating) OVER w_ordered AS running_max_rating,
        SUM(rating) OVER w_ordered AS running_sum_rating,
        CUME_DIST() OVER w_recent AS rating_cume_dist
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY city ORDER BY rating ASC)
),
remaining_keys AS (
    SELECT id, city
    FROM enriched
    EXCEPT ALL
    SELECT id, city
    FROM suppliers
    WHERE rating >= 4.0
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= '2025-08-08'
        )
),
not_flagged_suppliers AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty <> 323
        )
),
stats AS (
    SELECT
        CASE WHEN running_sum_rating >= 20.2 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_rating_tier,
        CASE WHEN running_max_rating >= 4.3 THEN 'HIGH' ELSE 'LOW' END AS running_max_rating_tier,
        city,
        ROUND(AVG(running_sum_rating), 2) AS avg_running_sum_rating,
        SIZE(COLLECT_SET(city)) AS num_cities,
        PERCENTILE_APPROX(rating_cume_dist, 0.5) AS rating_cume_dist_p50,
        COUNT(DISTINCT city) AS distinct_cities,
        SUM(rating_cume_dist) FILTER (WHERE city = 'London') AS london_total_rating_cume_dist
    FROM not_flagged_suppliers
    GROUP BY CASE WHEN running_sum_rating >= 20.2 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_max_rating >= 4.3 THEN 'HIGH' ELSE 'LOW' END, city
)
SELECT
    running_sum_rating_tier,
    running_max_rating_tier,
    city,
    avg_running_sum_rating,
    num_cities,
    rating_cume_dist_p50,
    distinct_cities,
    london_total_rating_cume_dist
FROM stats
