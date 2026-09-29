WITH flattened AS (SELECT id, effective_from, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(discounts) li AS val) SELECT id FROM flattened;
