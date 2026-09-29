SELECT
    ss.product_id,
    s.opened_date,
    s.region,
    s.city,
    LAST_DAY(ss.sale_date) AS sale_date_month_end
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
WHERE UPPER(s.store_name) LIKE '%ALL%'
