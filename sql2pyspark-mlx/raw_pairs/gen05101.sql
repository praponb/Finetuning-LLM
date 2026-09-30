WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        LEAST(price, cost) AS least_price_cost,
        LAST_DAY(launch_date) AS launch_date_month_end
    FROM products
    WHERE launch_date > DATE '2023-08-16'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(least_price_cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS least_price_cost_avg_5_prior,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        LAG(price, 1) OVER (PARTITION BY category ORDER BY least_price_cost, id) AS prev_price,
        NTILE(4) OVER (PARTITION BY supplier_id ORDER BY price, id) AS price_bucket
    FROM filtered
    WINDOW w_ordered AS (ORDER BY launch_date_month_end DESC, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price_bucket DESC, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY category ORDER BY launch_date_month_end DESC) AS dedup_rank_2
    FROM latest
    QUALIFY dedup_rank_2 = 1
)
SELECT
    s.id,
    s.category,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    s.launch_date_month_end,
    s.least_price_cost_avg_5_prior,
    s.event_seq,
    s.price_bucket,
    (SELECT SUM(p2.stock_qty) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS sum_product_stock_qty
FROM deduped s
ORDER BY id DESC NULLS LAST
LIMIT 20;
