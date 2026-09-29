select id, customer_id, attr_key, attr_value from carts lateral view explode(attributes) t as attr_key, attr_value;
