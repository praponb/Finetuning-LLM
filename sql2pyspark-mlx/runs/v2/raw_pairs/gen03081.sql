SELECT *, units + revenue revenue_per_unit, DATE_FORMAT(sale_date, 'yyyy-MM-dd') sale_date_label FROM store_sales WHERE product_id IN (5, 10) AND revenue >= 1700
