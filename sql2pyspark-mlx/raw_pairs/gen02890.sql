SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty ASC NULLS LAST, id ASC) AS latest_rank FROM products QUALIFY latest_rank = 1
