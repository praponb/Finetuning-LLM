select i.quantity, ss.revenue from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id where i.quantity <> 193 order by ss.revenue desc;
