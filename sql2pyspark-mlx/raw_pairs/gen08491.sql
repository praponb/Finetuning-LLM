select
    id,
    plan,
    explode_outer(discounts) as element
from price_tiers
