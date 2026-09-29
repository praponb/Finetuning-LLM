SELECT
    id,
    store_id,
    units,
    revenue
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) pos
    FROM store_sales
    WHERE store_id = 3
) r
WHERE pos = 1
ORDER BY store_id DESC, revenue DESC;
