SELECT sale_date, units, ROUND(units, 0) units_rounded FROM store_sales WHERE sale_date >= '2025-10-01' AND sale_date BETWEEN '2023-05-01' AND '2025-07-01' ORDER BY sale_date;
