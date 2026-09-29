SELECT revenue FROM store_sales UNION ALL SELECT unit_price AS revenue FROM order_items WHERE unit_price > 400 ORDER BY revenue
