SELECT
    ss.sale_date,
    ss.store_id,
    s.region,
    s.city,
    TRIM(s.store_name) AS clean_store_name
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
