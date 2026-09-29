select avg(units) avg_units, round(stddev(revenue), 2) stddev_revenue, sum(units) total_units from store_sales order by avg_units desc
