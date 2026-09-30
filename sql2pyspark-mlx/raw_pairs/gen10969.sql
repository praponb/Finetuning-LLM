with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, trim(promo_code) as clean_promo_code
    from promotions
    where starts_on >= date '2023-03-24'
      and channel in ('mobile', 'web')
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        ends_on,
        clean_promo_code,
        floor(discount_pct) as discount_pct_whole
    from filtered
    where ends_on > to_date('2023-08-03')
      and channel is not null
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct
    from cleaned a
    left join promotions pr
        on coalesce(a.channel, 'store') = pr.channel
       and a.ends_on >= pr.starts_on
       and a.ends_on < pr.ends_on + interval '14' day
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        ends_on,
        clean_promo_code,
        discount_pct_whole,
        promo_id,
        lag(discount_pct_whole, 1) over (partition by year(ends_on) order by discount_pct, id, promo_id) as prev_discount_pct_whole,
        ntile(5) over w_part as discount_pct_whole_bucket,
        sum(promo_id) over (order by promo_id, id, promo_id rows between unbounded preceding and current row) as running_promo_id
    from with_window
    window w_part as (partition by promo_code, promotion_promo_code order by discount_pct_whole, id, promo_id)
)
select
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct_whole,
    promo_id,
    prev_discount_pct_whole,
    discount_pct_whole_bucket,
    running_promo_id,
    case when promo_code rlike '^(SPRING10|WELCOME5)$' then 'GROUP_B' when promo_code rlike '^(VIP|BUNDLE)$' then 'OTHER_ZONE' else 'PRIORITY' end as promo_code_zone
from enriched;
