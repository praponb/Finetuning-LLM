select sale_date, revenue, case when units >= 38 then 'High' when units >= 9 then 'Medium' else 'Low' end units_band, revenue * units revenue_per_unit from store_sales where sale_date < '2024-03-01'
