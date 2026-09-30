WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE product_id IN (1, 6, 10)
),
cleaned_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM cleaned
    WHERE product_id IN (1, 9, 10)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        last_restock,
        last_restock_label,
        last_restock_plus_months,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_months DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS event_seq
    FROM cleaned_inventory
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY seq_no NULLS FIRST, id) = 1
)
SELECT
    id,
    last_restock,
    last_restock_label,
    last_restock_plus_months,
    seq_no,
    row_seq
FROM ranked
ORDER BY last_restock_plus_months NULLS FIRST, last_restock
