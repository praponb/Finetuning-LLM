select id, customer_id, created_at, key, attr_value from carts lateral view explode(attributes) t as key, attr_value
