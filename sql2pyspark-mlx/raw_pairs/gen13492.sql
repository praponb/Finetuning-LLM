SELECT id, plan, EXPLODE(discounts) AS element FROM price_tiers;
