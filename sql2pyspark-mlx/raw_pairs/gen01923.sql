WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        LAG(quantity, 1) OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY last_restock ASC, id ASC) AS prev_quantity,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('WEEK', last_restock), store_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        ROW_NUMBER() OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY last_restock ASC, id ASC) AS row_seq
    FROM inventory
)
SELECT
    id,
    first_known_quantity,
    row_seq
FROM enriched
