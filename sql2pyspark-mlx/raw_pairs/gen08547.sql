SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC NULLS FIRST, id) AS latest_rank
FROM products
QUALIFY latest_rank = 1
