WITH ValidStoreSales AS (
    SELECT *
    FROM store_sales
    WHERE store_id = 1
),
agg AS (
    SELECT
        store_id,
        QUARTER(sale_date) sale_quarter,
        MIN(sale_date) first_sale_date,
        COUNT(*) cnt
    FROM ValidStoreSales
    GROUP BY store_id, QUARTER(sale_date)
)
SELECT
    store_id,
    sale_quarter,
    first_sale_date,
    cnt
FROM agg;
