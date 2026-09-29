WITH lines AS (SELECT id, effective_from, currency, EXPLODE(tier_limits) AS val FROM price_tiers) SELECT effective_from, currency, val FROM lines ORDER BY val NULLS FIRST;
