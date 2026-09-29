select id, created_at, ship_to, attributes, transform(items, li -> li.product_id) as product_ids, size(attributes) as num_attributes from carts;
