select ss.id, ss.store_id, ss.revenue, ss.product_id, s.store_name from store_sales ss join stores s on ss.store_id = s.id where s.opened_date between '2024-03-01' and '2025-07-01'
