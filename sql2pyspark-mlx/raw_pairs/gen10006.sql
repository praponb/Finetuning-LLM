WITH metrics AS (
    SELECT
        *,
        MIN(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_rating,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating,
        MAX(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating,
        COUNT(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_rating
    FROM suppliers
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND YEAR(c.launch_date) = 2023
        )
),
latest AS (
    SELECT
        id,
        country,
        city,
        running_min_rating,
        running_max_rating,
        running_count_rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_count_rating DESC NULLS LAST, id) AS dedup_rank
        FROM eligible
    ) ranked
    WHERE dedup_rank = 1
),
remaining_keys AS (
    SELECT id
    FROM latest
    EXCEPT ALL
    SELECT id
    FROM suppliers
    WHERE rating <= 2.0
),
verified AS (
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.id = k.id
),
eligible_suppliers AS (
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= '2025-05-01'
        )
),
filtered AS (
    SELECT
        id,
        city,
        running_max_rating,
        running_count_rating,
        CASE WHEN running_count_rating >= 14 THEN 'High' WHEN running_count_rating >= 4 THEN 'Medium' ELSE 'Low' END AS running_count_rating_tier
    FROM eligible_suppliers
    WHERE city IS DISTINCT FROM 'Tokyo'
      AND running_min_rating > 3.5
)
SELECT
    city,
    running_max_rating,
    running_count_rating,
    running_count_rating_tier
FROM filtered
ORDER BY running_max_rating NULLS FIRST, running_count_rating
