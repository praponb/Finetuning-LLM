SELECT *, ROUND(units, 2) AS units_rounded, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales
WHERE units IS NOT NULL;
