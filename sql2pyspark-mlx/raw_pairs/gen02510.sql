WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(stock_qty) AS stock_qty_whole
    FROM products
    WHERE product_name NOT LIKE '%USB%'
      AND brand IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price BETWEEN 210 AND 270
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        COUNT(price) OVER w_grp AS running_count_price,
        SUM(price) OVER w_grp AS running_sum_price,
        AVG(price) OVER w_grp AS running_avg_price
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY category ORDER BY stock_qty_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY running_avg_price, id) AS seq_no,
        CORR(cost, price) OVER (PARTITION BY brand) AS corr_cost_price,
        AVG(running_sum_price) OVER (PARTITION BY brand) AS brand_avg_running_sum_price,
        LEAD(running_count_price, 1) OVER (PARTITION BY brand ORDER BY price, id) AS next_running_count_price
    FROM enriched
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price NULLS LAST, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
)
SELECT
    id,
    product_name,
    brand,
    cost,
    running_count_price,
    running_sum_price,
    running_avg_price,
    corr_cost_price,
    brand_avg_running_sum_price
FROM latest
ORDER BY id DESC NULLS FIRST, corr_cost_price
