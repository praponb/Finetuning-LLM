select shipping_fee, 'order' as source from orders where channel = 'mobile' union select unit_price as shipping_fee, 'order_item' as source from order_items where quantity <> 4;
