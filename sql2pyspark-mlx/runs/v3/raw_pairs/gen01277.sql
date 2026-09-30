SELECT *, ROUND(revenue, 2) AS revenue_rounded, DATE_TRUNC('week', sale_date) AS sale_date_period FROM store_sales WHERE revenue <= 2800
