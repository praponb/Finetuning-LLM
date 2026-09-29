with exploded as (
    select
        id,
        effective_from,
        currency,
        explode(tier_limits) as element
    from price_tiers
)
select
    currency,
    count(*) as event_count,
    sum(element) as total_element,
    count(*) as row_count,
    sum(case when currency = 'EUR' then element else 0 end) as eur_element,
    avg(element) filter (where currency = 'EUR') as eur_avg_element,
    sum(element) filter (where currency = 'USD') as usd_total_element
from exploded
group by currency
