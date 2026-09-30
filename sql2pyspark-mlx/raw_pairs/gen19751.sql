WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365.25 AS years_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-05-12')
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, quantity - years_since_last_restock AS quantity_minus_years_since_last_restock
    FROM base
    WHERE last_restock >= DATE '2023-06-03'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid
    INTERSECT ALL
    SELECT id
    FROM inventory
    WHERE quantity BETWEEN 106 AND 224
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        quantity,
        last_restock,
        years_since_last_restock,
        quantity_minus_years_since_last_restock,
        CASE WHEN quantity_minus_years_since_last_restock >= 65 THEN 'High' WHEN quantity_minus_years_since_last_restock >= 32 THEN 'Medium' ELSE 'Low' END AS quantity_minus_years_since_last_restock_band
    FROM reconciled
    WHERE last_restock >= DATE '2023-08-20'
),
scored AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (ORDER BY years_since_last_restock DESC NULLS LAST) AS years_since_last_restock_rank
    FROM cleaned
)
SELECT
    quantity,
    last_restock,
    quantity_minus_years_since_last_restock_band
FROM scored
ORDER BY quantity DESC NULLS LAST, last_restock DESC
