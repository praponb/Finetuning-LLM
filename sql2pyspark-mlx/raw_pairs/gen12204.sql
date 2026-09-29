select store_id, round(stddev(units), 2) as stddev_units, avg(units) as avg_units from store_sales where revenue >= 2500 group by store_id;
