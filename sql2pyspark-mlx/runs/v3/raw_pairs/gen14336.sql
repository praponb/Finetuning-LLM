select revenue, product_id, greatest(units, revenue) as greatest_units_revenue, round(revenue / units, 2) as revenue_per_unit from store_sales where units < 14;
