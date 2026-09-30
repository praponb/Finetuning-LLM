select id, revenue, last_day(sale_date) as sale_date_month_end from store_sales order by sale_date_month_end;
