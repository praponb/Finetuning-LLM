select revenue from store_sales where product_id in (5, 8, 9, 10) union select monthly_fee from subscriptions where start_date between '2024-12-01' and '2025-05-01'
