SELECT *, cost * stock_qty AS cost_times_stock_qty
FROM products
WHERE brand IN ('Acme', 'Initech', 'Umbrella') OR SIZE(tags) > 1
