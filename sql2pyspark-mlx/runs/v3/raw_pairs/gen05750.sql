SELECT *, CAST(revenue AS INT) AS revenue_int, COALESCE(units, 0) AS units_filled
FROM store_sales
WHERE (units IS NOT NULL AND units <> 36) OR units < 32
ORDER BY units DESC, id
LIMIT 1;
