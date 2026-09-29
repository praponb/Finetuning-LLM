WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(product_name)) AS product_name_title,
        DATEDIFF(CURRENT_DATE(), launch_date) / 365.25 AS years_since_launch_date
    FROM products
    WHERE launch_date > TO_DATE('2023-09-17')
      AND cost > 470
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY price DESC) AS row_num
        FROM cleaned
    ) t
    WHERE row_num = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        d.city,
        d.supplier_name,
        d.rating
    FROM ranked a
    LEFT JOIN suppliers d
        ON a.supplier_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY city ORDER BY cost DESC, id) AS cost_bucket,
        STDDEV_SAMP(years_since_launch_date) OVER (PARTITION BY supplier_id) AS supplier_id_stddev_samp_years_since_launch_date,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY rating, id) AS event_seq
    FROM joined
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.cost,
    s.stock_qty,
    s.supplier_id,
    s.product_name_title,
    s.row_num,
    CASE WHEN product_name RLIKE '^(Coffee Maker|Yoga Mat)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS product_name_zone,
    (SELECT SUM(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS sum_product_stock_qty
FROM scored s
ORDER BY row_num DESC
