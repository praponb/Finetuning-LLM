with lines as (
    select
        id,
        plan,
        currency,
        explode_outer(tier_prices) as val
    from price_tiers
)
select
    id,
    plan,
    val,
    rank() over (partition by currency, plan order by val desc) as val_rank,
    rank() over (order by val nulls last) as val_rank_2,
    percent_rank() over w_part as val_percent_rank
from lines
where currency = 'USD'
window w_part as (partition by plan order by val desc nulls first);
