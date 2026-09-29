with lines as (select id, plan, currency, explode(tier_prices) as val from price_tiers) select id, currency from lines order by currency, id;
