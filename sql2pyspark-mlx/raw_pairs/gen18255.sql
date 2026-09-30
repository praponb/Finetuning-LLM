WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(quantity AS STRING) AS quantity_num
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price < 220
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.unit_price,
        src.quantity_num,
        p.launch_date
    FROM valid src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE p.product_name LIKE '%Yog%'
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.price,
        p.stock_qty
    FROM joined s
    LEFT JOIN products p
        ON s.product_id = p.id
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY unit_price NULLS FIRST) AS unit_price_percent_rank,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY unit_price, id), w_hist AS (PARTITION BY product_id ORDER BY price DESC, id)
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
        AVG(stock_qty) AS avg_stock_qty,
        ROUND(AVG(unit_price_percent_rank), 2) AS avg_unit_price_percent_rank
    FROM enriched_order_items
    GROUP BY seq_no_tier
    HAVING AVG(stock_qty) >= 0
)
SELECT seq_no_tier
FROM grouped
