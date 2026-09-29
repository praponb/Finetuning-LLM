SELECT *, LAST_DAY(sale_date) AS sale_date_month_end FROM store_sales ORDER BY store_id DESC;
