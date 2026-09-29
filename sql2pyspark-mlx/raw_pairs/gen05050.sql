WITH flattened AS (SELECT id, effective_from, EXPLODE_OUTER(tier_limits) AS element FROM price_tiers) SELECT id, effective_from, element FROM flattened
