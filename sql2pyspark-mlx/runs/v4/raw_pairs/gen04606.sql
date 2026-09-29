SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS LAST, id) AS rn
FROM suppliers
QUALIFY rn = 1
