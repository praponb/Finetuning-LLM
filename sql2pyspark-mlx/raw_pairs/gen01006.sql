WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        last_restock,
        ROUND(quantity * 100, 2) adjusted_quantity
    FROM inventory
    WHERE last_restock > TO_DATE('2023-06-30')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        last_restock,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY adjusted_quantity DESC) adjusted_quantity_cume_dist,
        AVG(adjusted_quantity) OVER (PARTITION BY product_id ORDER BY adjusted_quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) adjusted_quantity_avg_5_prior,
        FIRST_VALUE(adjusted_quantity, TRUE) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_adjusted_quantity
    FROM base
),
base_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(adjusted_quantity_avg_5_prior, 0) adjusted_quantity_avg_5_prior_filled
    FROM with_windows
    WHERE last_restock >= DATE '2023-04-17'
      AND first_known_adjusted_quantity >= 43
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY adjusted_quantity_avg_5_prior DESC NULLS LAST, id) AS dedup_rank
        FROM base_inventory
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.product_id,
    s.last_restock,
    s.adjusted_quantity_cume_dist,
    s.adjusted_quantity_avg_5_prior,
    s.first_known_adjusted_quantity,
    (SELECT MAX(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS max_inventory_quantity
FROM deduped s
ORDER BY first_known_adjusted_quantity DESC NULLS LAST;
