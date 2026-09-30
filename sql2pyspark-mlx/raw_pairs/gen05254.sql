select id, customer_id, created_at, coupon_codes, items, ship_to, attributes, size(items) as num_items, coalesce(attributes['utm_source'], 'unknown') as utm_source_or_default from carts;
