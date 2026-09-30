WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        product_id,
        quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS row_num
        FROM order_items
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_avg_3,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS row_seq
    FROM deduped
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT s.*, d.stock_qty
    FROM enriched s
    INNER JOIN products d
        ON s.product_id = d.id
    WHERE d.price <= 1170
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM enriched_order_items
    EXCEPT
    SELECT product_id
    FROM order_items
    WHERE order_id = 20
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_order_items s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity_avg_3,
        row_seq,
        RANK() OVER w_ordered AS quantity_avg_3_rank,
        CUME_DIST() OVER (ORDER BY row_seq) AS row_seq_cume_dist,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity_avg_3 DESC) AS quantity_avg_3_rank_2,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY row_seq DESC NULLS LAST) AS row_seq_percent_rank,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY product_id), 4) AS row_seq_share
    FROM verified
    WINDOW w_ordered AS (ORDER BY quantity_avg_3 DESC)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        product_id,
        row_seq_percent_rank,
        row_seq_cume_dist
    FROM scored
) src
UNPIVOT (
    value FOR metric IN (row_seq_percent_rank, row_seq_cume_dist)
) long_rows
