SELECT id, customer_id, created_at, items, attributes, ARRAY_MAX(TRANSFORM(items, li -> li.price)) AS max_item_price, FORALL(items, li -> li.qty <= 5) AS all_small_qty FROM carts
