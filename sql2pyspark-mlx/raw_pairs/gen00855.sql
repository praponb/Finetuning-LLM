with flattened as (
    select
        id,
        effective_from,
        currency,
        explode(discounts) as val
    from price_tiers
),
filtered as (
    select *, date_format(effective_from, 'yyyy-MM') as effective_from_label
    from flattened
    where effective_from >= date '2024-06-03'
),
scored as (
    select
        *,
        cume_dist() over (partition by currency order by val nulls last) as val_cume_dist,
        min(val) over (partition by currency) as currency_min_val,
        rank() over (partition by currency order by val desc nulls last) as val_rank
    from filtered
    where val > 28
),
base as (
    select
        id,
        currency,
        val,
        effective_from_label,
        val_cume_dist,
        val_rank,
        cast(val_rank as double) as val_rank_str
    from scored
    where val_cume_dist >= 0.52
)
select
    case when val_rank >= 9 then 'LARGE' else 'SMALL' end as val_rank_tier,
    case when val_rank_str >= 72 then 'PREMIUM' else 'STANDARD' end as val_rank_str_tier,
    size(collect_set(currency)) as num_currencies,
    max(val) filter (where currency = 'THB') as thb_max_val,
    count(case when currency = 'THB' then 1 end) as thb_count,
    count_if(currency = 'EUR') as eur_events,
    count(*) as num_rows
from base
group by 1, 2
order by thb_count
