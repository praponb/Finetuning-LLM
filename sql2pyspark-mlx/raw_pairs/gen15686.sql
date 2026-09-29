WITH deduped AS (
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) = 1
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue
FROM deduped
ORDER BY units NULLS LAST;
