SELECT id, customer_id, items, attributes, attributes['device'] AS device, COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default FROM carts;
