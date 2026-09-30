select store_id, max(sale_date) as latest_sale_date, min(sale_date) as min_sale_date, round(stddev(units), 2) as stddev_units from store_sales group by store_id having stddev_units > 36;
