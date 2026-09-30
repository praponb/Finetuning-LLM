WITH filtered AS (
    SELECT
        *,
        COALESCE(revenue, 0) AS revenue_filled,
        WEEKOFYEAR(sale_date) AS sale_date_weekofyear
    FROM store_sales
    WHERE YEAR(sale_date) >= 2023
),
base AS (
    SELECT
        *,
        ROUND(revenue_filled / units, 2) AS revenue_filled_per_units,
        ROUND(revenue, 1) AS revenue_rounded
    FROM filtered
    WHERE sale_date >= DATE '2024-04-05'
      AND revenue > 2500
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date DESC NULLS LAST, id) AS rn
    FROM base
    QUALIFY rn = 1
),
cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_filled,
        sale_date_weekofyear,
        revenue_rounded,
        rn,
        units * revenue_filled_per_units AS units_times_revenue_filled_per_units
    FROM ranked
    WHERE sale_date >= DATE '2024-02-29'
)
SELECT
    CASE WHEN sale_date_weekofyear >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS sale_date_weekofyear_tier,
    ROUND(AVG(sale_date_weekofyear), 2) AS avg_sale_date_weekofyear,
    BIT_OR(units) AS units_bit_or,
    ROUND(AVG(sale_date_weekofyear), 2) AS avg_sale_date_weekofyear_2,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    BIT_OR(units) AS units_bit_or_2
FROM cleaned
GROUP BY 1
ORDER BY sale_date_weekofyear_tier DESC
