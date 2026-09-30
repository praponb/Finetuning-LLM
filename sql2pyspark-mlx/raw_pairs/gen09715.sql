WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.rating,
        x.product_stock_qty,
        x.product_price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        rating,
        product_stock_qty,
        product_price,
        CASE WHEN product_stock_qty >= 326 THEN 'High' WHEN product_stock_qty >= 146 THEN 'Medium' ELSE 'Low' END AS product_stock_qty_band,
        product_price / NULLIF(product_stock_qty, 0) AS product_price_per_product_stock_qty,
        COALESCE(city, 'OTHER') AS city_clean
    FROM with_last
    WHERE city <> 'Singapore'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY product_price, id) AS seq_no,
        AVG(product_stock_qty) OVER w_recent AS product_stock_qty_avg_2,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS seq_no_2,
        AVG(rating) OVER w_ordered AS rating_avg_6_prior,
        SUM(product_stock_qty) OVER (PARTITION BY city ORDER BY product_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_stock_qty
    FROM valid
    WINDOW w_recent AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(cumulative_product_stock_qty AS STRING) AS cumulative_product_stock_qty_str
    FROM with_windows
    WHERE rating_avg_6_prior <> 2
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        rating,
        product_stock_qty,
        product_price,
        seq_no,
        seq_no_2,
        rating_avg_6_prior,
        cumulative_product_stock_qty_str,
        SUM(product_price_per_product_stock_qty) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_product_price_per_product_stock_qty,
        SUM(seq_no) OVER (PARTITION BY city ORDER BY cumulative_product_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no,
        ROW_NUMBER() OVER (ORDER BY product_stock_qty_avg_2, id) AS seq_no_3,
        LAST_VALUE(product_price) IGNORE NULLS OVER (PARTITION BY city ORDER BY product_stock_qty_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_price,
        AVG(product_price) OVER (PARTITION BY city ORDER BY rating_avg_6_prior, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS product_price_avg_5_prior
    FROM filtered
)
SELECT
    id,
    rating,
    product_stock_qty,
    product_price,
    seq_no,
    seq_no_2,
    rating_avg_6_prior,
    cumulative_seq_no,
    seq_no_3
FROM scored
ORDER BY rating_avg_6_prior DESC NULLS FIRST
