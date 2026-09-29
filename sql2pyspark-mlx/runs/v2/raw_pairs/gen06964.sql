SELECT ss.id, ss.store_id, ss.units, ss.sale_date, p.cost, ROUND(ss.revenue * 1.1, 2) AS adjusted_revenue FROM store_sales ss JOIN products p ON p.id = ss.product_id
