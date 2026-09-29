with base as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        concat_ws('/', city, region) as city_region_key
    from stores
    where opened_date > date '2023-09-06'
),
cleaned as (
    select
        id,
        store_name,
        city,
        opened_date,
        sqft,
        city_region_key,
        concat(region, ' ', store_name) as region_store_name
    from base
    where opened_date >= date '2023-09-03'
      and region is distinct from 'West'
),
ranked as (
    select *
    from cleaned
    qualify row_number() over (partition by city order by opened_date, id) = 1
),
with_last as (
    select
        s.*,
        x.channel,
        x.order_discount
    from ranked s
    join lateral (
            select c.channel, c.discount as order_discount
            from orders c
            where c.store_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) x on true
),
attributed as (
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from with_last s
    left join promotions pr
        on coalesce(s.channel, 'web') = pr.channel
       and s.opened_date between pr.starts_on and pr.ends_on
),
attributed_stores as (
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from attributed a
    inner join promotions pr
        on a.channel = pr.channel
       and a.opened_date between pr.starts_on and pr.ends_on
),
daily as (
    select
        date_trunc('quarter', opened_date) as quarter_start,
        promo_code,
        sort_array(collect_set(promotion_promo_code)) as promotion_promo_code_list,
        count(distinct channel) as distinct_channels,
        max(discount_pct) as max_discount_pct,
        sort_array(collect_set(promo_code)) as promo_code_list,
        sum(sqft) filter (where promotion_promo_code = 'SPRING10') as spring10_total_sqft,
        count(*) filter (where city = 'Tokyo') as tokyo_rows,
        rank() over (partition by date_trunc('quarter', opened_date) order by count(distinct channel) desc) as distinct_channels_rank
    from attributed_stores
    group by date_trunc('quarter', opened_date), promo_code
    having tokyo_rows >= 1
)
select
    quarter_start,
    promo_code,
    distinct_channels,
    distinct_channels_rank
from daily
order by promo_code desc, distinct_channels_rank
