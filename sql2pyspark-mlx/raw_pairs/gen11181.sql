WITH scored AS (
    SELECT
        *,
        STDDEV_SAMP(price) OVER (PARTITION BY category) AS category_stddev_samp_price,
        PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC NULLS LAST) AS cost_percent_rank,
        ROW_NUMBER() OVER (ORDER BY cost, id) AS seq_no
    FROM products
    WHERE price BETWEEN 370 AND 620
),
with_windows AS (
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY brand ORDER BY category_stddev_samp_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        SUM(cost) OVER (PARTITION BY YEAR(launch_date), brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        ROW_NUMBER() OVER (ORDER BY price DESC, id) AS seq_no_2
    FROM scored
),
base AS (
    SELECT *, category_stddev_samp_price + seq_no AS category_stddev_samp_price_plus_seq_no
    FROM with_windows
    WHERE stock_qty > 179
),
joined AS (
    SELECT
        a.*,
        p.rating,
        p.supplier_name,
        p.city
    FROM base a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
),
with_dims AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.supplier_id,
        s.category_stddev_samp_price,
        s.cost_percent_rank,
        s.seq_no,
        s.running_seq_no,
        s.running_cost,
        s.seq_no_2,
        s.rating,
        s.city,
        b.city AS supplier_city,
        b.supplier_name AS supplier_supplier_name,
        b.country
    FROM joined s
    INNER JOIN suppliers b
        ON s.supplier_id = b.id
),
kept AS (
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id IN (1, 9, 10)
        )
),
kept_products AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
        )
)
SELECT
    id,
    product_name,
    category,
    brand,
    stock_qty,
    supplier_id,
    category_stddev_samp_price,
    cost_percent_rank,
    seq_no,
    supplier_supplier_name,
    country,
    LAG(price, 1) OVER (PARTITION BY DATE_TRUNC('MONTH', launch_date), supplier_city ORDER BY seq_no, id) AS prev_price,
    SUM(cost_percent_rank) OVER (PARTITION BY city ORDER BY category_stddev_samp_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_percent_rank
FROM kept_products;
