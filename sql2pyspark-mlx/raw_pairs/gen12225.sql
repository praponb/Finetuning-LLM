WITH ranked AS (
    SELECT id, supplier_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC, id) AS rank_in_group
        FROM suppliers
    ) x
    WHERE rank_in_group = 1
),
cleaned AS (
    SELECT *, LOWER(supplier_name) supplier_name_lower
    FROM ranked
    WHERE supplier_name IS NOT NULL
),
valid AS (
    SELECT
        *,
        LOWER(supplier_name) supplier_name_lower_2,
        LENGTH(supplier_name) supplier_name_length
    FROM cleaned
    WHERE supplier_name LIKE 'To%'
),
with_windows AS (
    SELECT
        *,
        COUNT(supplier_name_length) OVER w_running last7_count_supplier_name_length,
        MAX(supplier_name_length) OVER w_running last7_max_supplier_name_length,
        MIN(supplier_name_length) OVER w_running last7_min_supplier_name_length
    FROM valid
    WINDOW w_running AS (ORDER BY supplier_name_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN last7_max_supplier_name_length >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_max_supplier_name_length_tier,
    CASE WHEN last7_count_supplier_name_length >= 14 THEN 'LARGE' ELSE 'SMALL' END AS last7_count_supplier_name_length_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(last7_count_supplier_name_length) AS max_last7_count_supplier_name_length,
    MIN(last7_min_supplier_name_length) AS min_last7_min_supplier_name_length,
    PERCENTILE_APPROX(supplier_name_length, ARRAY(0.25, 0.5, 0.75)) AS supplier_name_length_quartiles
FROM with_windows
GROUP BY 1, 2
HAVING COUNT(DISTINCT id) >= 1;
