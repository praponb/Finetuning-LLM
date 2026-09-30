WITH cleaned AS (
    SELECT
        id,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label,
        ROUND(quantity * 1.15, 2) AS adjusted_quantity
    FROM inventory
    WHERE quantity >= 86
),
base AS (
    SELECT
        *,
        COALESCE(adjusted_quantity, 0) AS adjusted_quantity_filled,
        COALESCE(adjusted_quantity, 0) AS adjusted_quantity_filled_2
    FROM cleaned
    WHERE last_restock >= TO_DATE('2023-05-22')
      AND last_restock BETWEEN '2024-09-01' AND '2025-03-14'
),
base_inventory AS (
    SELECT *, adjusted_quantity - adjusted_quantity_filled AS adjusted_quantity_minus_adjusted_quantity_filled
    FROM base
    WHERE last_restock >= DATE '2023-05-09'
      AND adjusted_quantity_filled <= 54
),
enriched AS (
    SELECT
        *,
        COUNT(adjusted_quantity_filled) OVER w_running AS last7_count_adjusted_quantity_filled,
        MIN(adjusted_quantity_filled) OVER w_running AS last7_min_adjusted_quantity_filled,
        MAX(adjusted_quantity_filled) OVER w_running AS last7_max_adjusted_quantity_filled,
        AVG(adjusted_quantity_filled) OVER w_running AS last7_avg_adjusted_quantity_filled
    FROM base_inventory
    WHERE adjusted_quantity_filled >= 64
    WINDOW w_running AS (ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
cleaned_inventory AS (
    SELECT
        id,
        last_restock,
        adjusted_quantity,
        adjusted_quantity_filled,
        adjusted_quantity_filled_2,
        adjusted_quantity_minus_adjusted_quantity_filled,
        last7_count_adjusted_quantity_filled,
        last7_avg_adjusted_quantity_filled,
        ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
    FROM enriched
    WHERE adjusted_quantity_minus_adjusted_quantity_filled IS NOT NULL
)
SELECT
    id,
    last_restock,
    adjusted_quantity,
    adjusted_quantity_filled_2,
    last7_count_adjusted_quantity_filled,
    last7_avg_adjusted_quantity_filled,
    last_restock_plus_months
FROM cleaned_inventory
ORDER BY last7_avg_adjusted_quantity_filled DESC
