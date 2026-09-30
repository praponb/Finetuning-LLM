WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        CASE WHEN quantity >= 262 THEN 'High' WHEN quantity >= 106 THEN 'Medium' ELSE 'Low' END quantity_tier
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-05-20')
      AND store_id = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        DATE_SUB(last_restock, 26) last_restock_minus_26d,
        ADD_MONTHS(last_restock, 1) last_restock_plus_months
    FROM filtered
    WHERE last_restock >= TO_DATE('2023-03-06')
      AND store_id IN (3, 4)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock_plus_months DESC, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        last_restock_minus_26d,
        last_restock_plus_months
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY rn DESC, id) = 1
),
latest_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        last_restock_minus_26d
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_plus_months NULLS LAST, id) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
valid_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_ADD(last_restock_minus_26d, 52) last_restock_minus_26d_plus_52d
    FROM latest_inventory
    WHERE last_restock_minus_26d > '2025-04-01'
)
SELECT
    id,
    store_id,
    last_restock_minus_26d_plus_52d
FROM valid_inventory
