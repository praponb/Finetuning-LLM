WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST, id) AS latest_rank
    FROM inventory
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.store_id,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.store_id = s.store_id) AS max_order_amount
FROM deduped s
ORDER BY store_id DESC NULLS LAST;
