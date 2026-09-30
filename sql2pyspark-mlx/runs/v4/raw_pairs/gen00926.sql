with latest as (
    select *, row_number() over (partition by promo_code order by discount_pct desc, id) as dedup_rank
    from promotions
    qualify dedup_rank = 1
)
select
    date_trunc('year', ends_on) as year_start,
    channel,
    count_if(channel = 'partner') as partner_events,
    round(sum(discount_pct) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as discount_pct_per_partner,
    round(sum(discount_pct) / nullif(count(case when channel = 'web' then 1 end), 0), 2) as discount_pct_per_web,
    avg(discount_pct) as avg_discount_pct,
    sort_array(collect_set(channel)) as channel_list,
    rank() over (partition by date_trunc('year', ends_on) order by avg(discount_pct) desc) as avg_discount_pct_rank
from latest
group by year_start, channel;
