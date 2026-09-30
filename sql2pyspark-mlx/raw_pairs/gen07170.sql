WITH exploded AS (SELECT id, plan, effective_from, EXPLODE(discounts) AS element FROM price_tiers) SELECT plan, effective_from, element FROM exploded ORDER BY element DESC NULLS LAST;
