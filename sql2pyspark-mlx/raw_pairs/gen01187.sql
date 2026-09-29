WITH valid AS (
    SELECT
        *,
        TRIM(brand) AS clean_brand,
        LOWER(brand) AS brand_lower
    FROM products
    WHERE launch_date >= TO_DATE('2023-04-04')
      AND cost >= 640
),
with_dims AS (
    SELECT
        src.*,
        s.supplier_name,
        s.city,
        s.rating
    FROM valid src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
metrics AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY supplier_id) AS supplier_id_sum_rating,
        ROUND(price / SUM(price) OVER (PARTITION BY supplier_id), 4) AS price_share
    FROM with_dims
),
metrics_products AS (
    SELECT
        *,
        LAST_VALUE(supplier_id_sum_rating) IGNORE NULLS OVER (PARTITION BY MONTH(launch_date), city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_supplier_id_sum_rating,
        AVG(price_share) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS price_share_avg_3,
        AVG(cost) OVER w_grp AS cost_avg_6,
        AVG(price) OVER (PARTITION BY city ORDER BY supplier_id_sum_rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS price_avg_3,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share_2
    FROM metrics
    WINDOW w_grp AS (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        launch_date,
        tags,
        supplier_name,
        city,
        rating,
        price_share,
        last_known_supplier_id_sum_rating,
        price_share_avg_3,
        cost_avg_6,
        price_avg_3,
        price_share_2,
        DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date,
        CAST(stock_qty AS DOUBLE) AS stock_qty_num
    FROM metrics_products
    WHERE launch_date > DATE '2023-03-06'
      AND city IS DISTINCT FROM 'Tokyo'
)
SELECT
    id,
    product_name,
    category,
    brand,
    cost,
    tags,
    supplier_name,
    city,
    rating,
    CASE WHEN supplier_name RLIKE '^(EuroGoods GmbH|Tokyo Traders)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS supplier_name_zone
FROM base
