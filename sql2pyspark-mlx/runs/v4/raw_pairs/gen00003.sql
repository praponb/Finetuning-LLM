WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 2.0 THEN 'Medium' ELSE 'Low' END rating_band,
        COALESCE(city, 'UNKNOWN') city_clean
    FROM suppliers
    WHERE supplier_name IS NOT NULL
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.category,
        top1.cost
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.category, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY rating DESC NULLS FIRST, id) = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT city
    FROM deduped
    EXCEPT
    SELECT city
    FROM suppliers
    WHERE supplier_name NOT LIKE 'S%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.city = k.city
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        city_clean,
        category,
        cost,
        COALESCE(supplier_name, 'N/A') supplier_name_filled
    FROM verified
    WHERE category IS NOT NULL
      AND rating >= 2.8
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY category ORDER BY cost DESC NULLS LAST) cost_rank,
        RANK() OVER (PARTITION BY category ORDER BY cost NULLS FIRST) cost_rank_2,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY cost DESC) cost_dense_rank,
        SUM(cost) OVER (PARTITION BY city) city_sum_cost,
        MAX(cost) OVER (PARTITION BY category, city) category_max_cost
    FROM base
)
-- Multi-level aggregation
SELECT
    city,
    category,
    CASE WHEN cost_rank_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS cost_rank_2_tier,
    AVG(cost_rank) AS avg_cost_rank,
    SUM(category_max_cost) FILTER (WHERE category = 'Books') AS books_total_category_max_cost,
    ROUND(SUM(cost_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS cost_dense_rank_per_london,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY CUBE (city, category, CASE WHEN cost_rank_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END)
HAVING SUM(category_max_cost) FILTER (WHERE category = 'Books') >= 0;
