select product_id from store_sales where units >= 12 union all select product_id from store_sales where units between 27 and 37
