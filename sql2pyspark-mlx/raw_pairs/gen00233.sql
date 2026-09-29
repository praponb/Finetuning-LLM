WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock,
        ROUND(quantity, 1) AS quantity_rounded
    FROM inventory
    WHERE last_restock BETWEEN '2025-05-01' AND '2025-10-01'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        days_since_last_restock,
        quantity_rounded
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_rounded, id) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
valid_inventory AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(days_since_last_restock, 2) AS days_since_last_restock_rounded
    FROM latest
    WHERE days_since_last_restock != 60
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        days_since_last_restock,
        quantity_rounded,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        FIRST_VALUE(days_since_last_restock_rounded) IGNORE NULLS OVER (ORDER BY days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_last_restock_rounded,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity, id) AS row_seq,
        SUM(days_since_last_restock_rounded) OVER (PARTITION BY store_id ORDER BY days_since_last_restock_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_last_restock_rounded
    FROM valid_inventory
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY days_since_last_restock DESC, id)
)
SELECT
    id,
    quantity,
    days_since_last_restock,
    quantity_rounded,
    event_seq,
    first_known_days_since_last_restock_rounded,
    row_seq,
    cumulative_days_since_last_restock_rounded
FROM metrics
ORDER BY quantity_rounded DESC NULLS LAST;
