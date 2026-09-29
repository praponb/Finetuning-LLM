SELECT id, units
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS LAST, id) AS rank_in_group
    FROM store_sales
) x
WHERE rank_in_group = 1
