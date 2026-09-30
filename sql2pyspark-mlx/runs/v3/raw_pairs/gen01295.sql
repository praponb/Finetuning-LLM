SELECT ss.product_id, ss.store_id, ss.revenue, s.opened_date, LAST_DAY(ss.sale_date) AS sale_date_month_end FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id
