select ss.id, ss.revenue from store_sales ss where year(ss.sale_date) >= 2025 and ss.store_id in (select id from stores where store_name like '%Airport%')
