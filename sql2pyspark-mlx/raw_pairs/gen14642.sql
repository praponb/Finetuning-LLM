WITH enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity,
        s.last_restock,
        p.price,
        p.launch_date,
        p.brand
    FROM inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.brand = 'Initech'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        brand,
        LAG(price) OVER w_grp AS prev_price,
        LEAD(price) OVER w_grp AS next_price,
        LAG(quantity) OVER w_grp AS prev_quantity,
        LEAD(quantity) OVER w_grp AS next_quantity,
        ROW_NUMBER() OVER w_grp AS seq_no,
        LAST_VALUE(quantity) IGNORE NULLS OVER w_ordered AS last_known_quantity
    FROM enriched
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY last_restock, id), w_ordered AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_inventory AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.last_restock,
        s.brand,
        s.prev_price,
        s.next_price,
        s.next_quantity,
        s.last_known_quantity,
        p.product_name,
        p.cost,
        p.brand AS product_brand
    FROM scored s
    LEFT JOIN products p
        ON s.product_id = p.id
),
enriched_inventory_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last_known_quantity) OVER (ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_quantity,
        NTILE(3) OVER (PARTITION BY product_brand ORDER BY prev_price, id) AS prev_price_bucket
    FROM enriched_inventory
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, product_brand ORDER BY cumulative_last_known_quantity DESC NULLS LAST, id) AS row_num
        FROM enriched_inventory_2
    ) ranked
    WHERE row_num = 1
),
enriched_inventory_3 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        brand,
        next_price,
        next_quantity,
        last_known_quantity,
        product_name,
        product_brand,
        prev_price_bucket,
        row_num,
        NTILE(4) OVER (PARTITION BY YEAR(last_restock), product_id ORDER BY next_quantity, id) AS next_quantity_bucket,
        SUM(prev_price_bucket) OVER (PARTITION BY brand ORDER BY prev_price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_price_bucket,
        PERCENT_RANK() OVER (ORDER BY prev_price_bucket DESC NULLS LAST) AS prev_price_bucket_percent_rank,
        NTILE(10) OVER (ORDER BY prev_price_bucket DESC, id) AS prev_price_bucket_bucket,
        MAX(next_quantity) OVER (PARTITION BY product_brand, brand) AS product_brand_max_next_quantity
    FROM latest
),
scored_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        next_price,
        next_quantity,
        last_known_quantity,
        row_num,
        running_prev_price_bucket,
        AVG(product_brand_max_next_quantity) OVER (PARTITION BY product_brand) AS product_brand_avg_product_brand_max_next_quantity,
        SUM(prev_price_bucket) OVER (PARTITION BY brand) AS brand_sum_prev_price_bucket,
        LAST_VALUE(next_quantity_bucket, TRUE) OVER (PARTITION BY product_brand ORDER BY prev_price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_quantity_bucket,
        AVG(product_brand_max_next_quantity) OVER (PARTITION BY brand) AS brand_avg_product_brand_max_next_quantity
    FROM enriched_inventory_3
)
SELECT
    id,
    last_restock,
    next_price,
    last_known_quantity,
    row_num,
    running_prev_price_bucket,
    product_brand_avg_product_brand_max_next_quantity,
    brand_sum_prev_price_bucket,
    last_known_next_quantity_bucket
FROM scored_inventory
ORDER BY last_known_quantity DESC NULLS FIRST;
