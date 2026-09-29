SELECT revenue FROM store_sales WHERE units < 11 EXCEPT SELECT cost AS revenue FROM shipments ORDER BY revenue
