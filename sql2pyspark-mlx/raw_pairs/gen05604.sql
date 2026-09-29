WITH lines AS (SELECT id, plan, EXPLODE_OUTER(discounts) AS val FROM price_tiers) SELECT id, plan, val FROM lines ORDER BY id DESC NULLS LAST;
