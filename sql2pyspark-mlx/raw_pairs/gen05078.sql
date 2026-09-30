WITH exploded AS (SELECT id, plan, EXPLODE(tier_prices) AS val FROM price_tiers) SELECT id, plan, RANK() OVER (ORDER BY val NULLS FIRST) val_rank FROM exploded;
