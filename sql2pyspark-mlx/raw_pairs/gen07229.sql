SELECT units, LEAST(units, revenue) least_units_revenue FROM store_sales WHERE units <= 17 ORDER BY least_units_revenue;
