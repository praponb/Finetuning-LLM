SELECT *, LAST_DAY(opened_date) opened_date_month_end, UPPER(TRIM(store_name)) store_name_normalized FROM stores ORDER BY 2 DESC, id LIMIT 50;
