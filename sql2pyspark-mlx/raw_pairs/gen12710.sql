select
    id,
    plan,
    effective_from,
    currency,
    explode(tier_prices) as val
from price_tiers;
