select revenue, round(revenue / units, 2) revenue_per_unit, datediff(current_date(), sale_date) days_since_sale_date from store_sales where units between 29 and 39;
