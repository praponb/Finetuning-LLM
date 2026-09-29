SELECT *, CASE WHEN status IN ('LOST', 'IN_TRANSIT') THEN 'Tier 1' ELSE 'Other' END status_group FROM shipments ORDER BY order_id
