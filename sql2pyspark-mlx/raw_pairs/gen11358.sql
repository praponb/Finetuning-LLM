with flattened as (
    select
        id,
        currency,
        val
    from price_tiers
    lateral view outer explode(discounts) li as val
)
select
    case when val >= 67 then 'LARGE' else 'SMALL' end as val_tier,
    currency,
    round(sum(val) / nullif(count(case when currency = 'THB' then 1 end), 0), 2) as val_per_thb,
    count(*) as event_count,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    sort_array(collect_set(currency)) as currency_list
from flattened
group by grouping sets ((case when val >= 67 then 'LARGE' else 'SMALL' end, currency), (case when val >= 67 then 'LARGE' else 'SMALL' end))
order by val_per_thb desc;
