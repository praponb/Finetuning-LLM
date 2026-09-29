SELECT id, status, channel FROM orders WHERE channel IN ('web', 'store', 'mobile') INTERSECT SELECT id, status, channel FROM orders WHERE store_id IS NOT NULL ORDER BY status;
