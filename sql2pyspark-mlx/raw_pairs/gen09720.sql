WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        LOWER(brand) AS brand_lower,
        SUBSTRING(category, 1, 4) AS category_prefix,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM products
    WHERE supplier_id IN (12, 24, 30)
      AND launch_date > DATE '2023-03-17'
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.category,
        src.brand,
        src.cost,
        src.launch_date,
        src.supplier_id,
        src.brand_lower,
        src.resolved_brand,
        p.city,
        p.rating,
        p.supplier_name
    FROM base src
    INNER JOIN suppliers p
        ON src.supplier_id <=> p.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(brand) AS brand_length
    FROM enriched
    WHERE launch_date > DATE '2023-09-07'
      AND supplier_id IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity <= 3
        )
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.brand,
        a.launch_date,
        a.supplier_id,
        a.brand_lower,
        a.city,
        a.rating,
        a.brand_length,
        p.country,
        p.supplier_name AS supplier_supplier_name
    FROM kept a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(launch_date), brand ORDER BY brand_length DESC, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY brand, country ORDER BY brand_length DESC, id) AS brand_length_bucket
    FROM with_dims
    WHERE rating BETWEEN 2.2 AND 4.1
),
kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM metrics
    EXCEPT ALL
    SELECT supplier_id
    FROM products
    WHERE supplier_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        city,
        ROUND(SUM(brand_length_bucket) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS brand_length_bucket_per_acme,
        COUNT_IF(brand = 'Globex') AS globex_events,
        SUM(CASE WHEN country = 'TH' THEN seq_no ELSE 0 END) AS th_seq_no,
        SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
        COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count
    FROM kept_rows
    GROUP BY CUBE (city)
    HAVING ROUND(SUM(brand_length_bucket) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) > 0
)
SELECT
    city,
    brand_length_bucket_per_acme,
    th_seq_no,
    brand_list,
    umbrella_count,
    CASE WHEN city RLIKE '^(Berlin|Tokyo)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'GROUP_A' END AS city_zone
FROM daily
ORDER BY umbrella_count, city_zone;
