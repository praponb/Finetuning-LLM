with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view explode(discounts) e as tier_value
)
-- Ranking and gap features
select
    id,
    effective_from,
    tier_value,
    avg(tier_value) over (partition by month(effective_from), currency) as effective_from_month_avg_tier_value
from lines
