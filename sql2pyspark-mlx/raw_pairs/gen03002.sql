WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        units,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE units = 22
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC NULLS FIRST, id) AS dedup_rank
FROM base
QUALIFY dedup_rank = 1
