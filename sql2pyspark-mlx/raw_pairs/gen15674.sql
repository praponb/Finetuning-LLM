select id, customer_id, key, value from carts lateral view outer explode(attributes) x as key, value
