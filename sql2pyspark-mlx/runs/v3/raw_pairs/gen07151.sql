select *, ceil(unit_price) as unit_price_whole from order_items where quantity <= 7 and (product_id is not null or product_id is null) order by 2
