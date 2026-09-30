select product_id, max(quantity) - min(quantity) as quantity_range from order_items group by product_id having max(quantity) - min(quantity) < 40 order by product_id desc
