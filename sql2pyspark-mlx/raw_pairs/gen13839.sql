select id, customer_id, ship_to, attributes, attributes['gift'] as gift, attributes['device'] as device, ship_to.city as ship_city from carts;
