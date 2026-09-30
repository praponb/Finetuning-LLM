WITH with_dims AS (
    SELECT
        e.*,
        b.brand,
        b.product_name,
        b.stock_qty
    FROM store_sales e
    LEFT JOIN products b
        ON e.product_id = b.id
    WHERE b.stock_qty = 290
),
scored AS (
    SELECT
        *,
        LAG(units) OVER (PARTITION BY brand ORDER BY units, id) prev_units,
        LEAD(stock_qty) OVER (PARTITION BY brand ORDER BY units, id) next_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY units, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) event_seq
    FROM with_dims
),
enriched AS (
    SELECT
        s.*,
        d.stock_qty AS product_stock_qty,
        d.price,
        d.brand AS product_brand
    FROM scored s
    LEFT JOIN products d
        ON s.product_id = d.id
),
with_windows AS (
    SELECT
        *,
        LEAD(product_stock_qty) OVER w_grp next_product_stock_qty,
        LAG(product_stock_qty) OVER w_grp prev_product_stock_qty,
        LAG(stock_qty) OVER w_grp prev_stock_qty,
        ROW_NUMBER() OVER w_grp seq_no_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id, store_id ORDER BY event_seq, id)
),
scored_store_sales AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        product_name,
        stock_qty,
        prev_units,
        next_stock_qty,
        event_seq,
        price,
        product_brand,
        next_product_stock_qty,
        prev_stock_qty,
        seq_no_2,
        ROW_NUMBER() OVER w_ordered event_seq_2,
        LAST_VALUE(next_stock_qty) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_next_stock_qty,
        SUM(event_seq) OVER (PARTITION BY brand ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_event_seq
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY MONTH(sale_date), store_id ORDER BY sale_date, id)
),
matched_keys AS (
    SELECT id, product_id
    FROM scored_store_sales
    INTERSECT ALL
    SELECT id, product_id
    FROM store_sales
    WHERE units BETWEEN 32 AND 37
),
kept_rows AS (
    SELECT s.*
    FROM scored_store_sales s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
)
SELECT
    p.brand,
    e.product_brand,
    COUNT(*) AS rows_total,
    AVG(e.prev_stock_qty) AS avg_prev_stock_qty,
    MIN(e.seq_no_2) AS min_seq_no_2
FROM kept_rows e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY GROUPING SETS ((p.brand, e.product_brand), (p.brand), ());
