WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 30 AS months_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-03-28')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        months_since_last_restock,
        LEAD(months_since_last_restock) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_months_since_last_restock,
        LEAD(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_quantity,
        LAG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS row_seq
    FROM base
    WHERE quantity BETWEEN 161 AND 200
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_months_since_last_restock DESC NULLS FIRST, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY store_id ORDER BY row_seq DESC) = 1
)
SELECT
    s.id,
    s.store_id,
    s.quantity,
    s.last_restock,
    s.months_since_last_restock,
    s.next_quantity,
    s.row_num,
    (SELECT AVG(t2.amount) FROM orders t2 WHERE t2.store_id = s.store_id) AS avg_order_amount
FROM latest s
