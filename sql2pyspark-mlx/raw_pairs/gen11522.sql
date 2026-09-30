WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label,
        FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2023-08-17'
      AND product_id = 8
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        LEAD(quantity_whole) OVER w_running AS next_quantity_whole,
        LAG(quantity_whole) OVER w_running AS prev_quantity_whole,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM valid
    WHERE quantity_whole < 56
    WINDOW w_running AS (ORDER BY quantity_whole, id)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(last_restock, 12) AS last_restock_plus_months
    FROM scored
    WHERE last_restock >= TO_DATE('2023-08-21')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT
    SELECT id
    FROM inventory
    WHERE quantity = 127
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    last_restock,
    last_restock_plus_months
FROM verified
ORDER BY id;
