SELECT *, DATE_TRUNC('week', launch_date) launch_date_period FROM products WHERE stock_qty >= 277 ORDER BY price DESC, category
