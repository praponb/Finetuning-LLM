select id, customer_id, key, attr_value from carts lateral view outer explode(attributes) e as key, attr_value;
