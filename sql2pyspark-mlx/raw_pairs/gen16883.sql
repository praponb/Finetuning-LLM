select
    id,
    plan,
    effective_from,
    element
from price_tiers
lateral view outer explode(discounts) t as element;
