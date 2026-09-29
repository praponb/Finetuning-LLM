WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity * 1.07, 2) AS quantity_adjusted
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-05-29')
      AND quantity >= 228
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.store_name,
        d.region,
        d.sqft
    FROM valid s
    JOIN stores d
        ON s.store_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity,
        LEAD(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity,
        LEAD(quantity_adjusted) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity_adjusted,
        LAG(quantity_adjusted) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity_adjusted,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS event_seq
    FROM joined
    WHERE region <> 'Central'
),
valid_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        quantity_adjusted,
        store_name,
        sqft,
        prev_quantity,
        next_quantity,
        next_quantity_adjusted,
        prev_quantity_adjusted,
        event_seq,
        CASE WHEN prev_quantity >= 199 THEN 'High' WHEN prev_quantity >= 78 THEN 'Medium' ELSE 'Low' END AS prev_quantity_level
    FROM with_windows
    WHERE region IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY next_quantity_adjusted NULLS LAST) AS rank_in_group
        FROM valid_inventory
    ) x
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM deduped
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE store_id IN (2, 3)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN matched_keys k ON s.product_id = k.product_id AND s.id = k.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        product_id,
        next_quantity_adjusted,
        prev_quantity_adjusted
    FROM kept_rows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (next_quantity_adjusted, prev_quantity_adjusted)
) long_rows;
