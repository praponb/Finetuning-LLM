with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by channel, promo_code order by starts_on, id) as row_num
    from promotions
    qualify row_num = 1
)
-- Multi-level aggregation
select
    case when discount_pct >= 0.27 then 'PREMIUM' else 'STANDARD' end as discount_pct_tier,
    channel,
    round(avg(discount_pct), 2) as avg_discount_pct,
    round(avg(discount_pct), 2) as avg_discount_pct_2,
    count(*) filter (where channel = 'mobile') as mobile_rows,
    count(*) as cnt,
    round(sum(discount_pct) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as discount_pct_per_mobile,
    grouping_id() as grouping_id
from ranked
group by cube (case when discount_pct >= 0.27 then 'PREMIUM' else 'STANDARD' end, channel);
