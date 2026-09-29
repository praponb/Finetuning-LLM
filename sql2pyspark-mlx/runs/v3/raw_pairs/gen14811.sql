select id, units, revenue, round(revenue / units, 2) as revenue_per_unit from store_sales where revenue > 3700 and sale_date >= '2024-04-09' order by 1 desc, revenue_per_unit desc;
