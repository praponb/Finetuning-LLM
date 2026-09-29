WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(quantity) OVER w_hist AS prev_quantity,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM inventory
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY last_restock ASC, id ASC)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, seq_no / NULLIF(quantity, 0) AS seq_no_per_quantity
    FROM metrics
    WHERE product_id IN (1, 4, 6, 10)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.store_name,
        d.opened_date
    FROM cleaned src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT e.*, d.sqft
    FROM enriched e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock ASC NULLS LAST) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    prev_quantity,
    seq_no,
    seq_no_per_quantity,
    store_name,
    opened_date
FROM deduped;
