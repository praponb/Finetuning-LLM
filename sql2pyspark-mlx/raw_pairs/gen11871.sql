WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        unit_price,
        ROUND(unit_price * 100, 2) adjusted_unit_price,
        CASE WHEN quantity >= 6 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END quantity_tier
    FROM order_items
    WHERE quantity < 7
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(adjusted_unit_price) adjusted_unit_price_whole,
        COALESCE(unit_price, 0) unit_price_filled
    FROM filtered
    WHERE product_id IS NOT NULL
      AND adjusted_unit_price < 58
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        b.category,
        b.stock_qty
    FROM cleaned s
    LEFT JOIN products b
        ON s.product_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(stock_qty) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price_whole, id) prev_stock_qty,
        LEAD(unit_price_filled) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price_whole, id) next_unit_price_filled,
        LAG(unit_price_filled) OVER (PARTITION BY product_id ORDER BY adjusted_unit_price_whole, id) prev_unit_price_filled,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY adjusted_unit_price_whole, id) event_seq
    FROM joined
)
-- Ranking and gap features
SELECT
    *,
    LEAD(next_unit_price_filled, 1) OVER w_recent next_next_unit_price_filled,
    LAG(prev_stock_qty, 1, 0) OVER w_recent prev_prev_stock_qty,
    AVG(unit_price_filled) OVER (PARTITION BY category ORDER BY adjusted_unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) unit_price_filled_avg_6_prior,
    RANK() OVER w_grp adjusted_unit_price_rank
FROM with_windows
WINDOW w_recent AS (PARTITION BY product_id ORDER BY stock_qty, id), w_grp AS (PARTITION BY product_id ORDER BY adjusted_unit_price DESC NULLS LAST);
