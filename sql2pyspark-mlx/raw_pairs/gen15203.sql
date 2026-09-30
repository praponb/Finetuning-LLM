with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        region,
        opened_date,
        sqft,
        case when sqft >= 14106 then 'High' when sqft >= 6032 then 'Medium' else 'Low' end as sqft_level
    from stores
    where opened_date >= date '2023-09-06'
      and opened_date between '2023-08-01' and '2023-11-01'
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        s.region,
        s.sqft_level,
        top1.order_status,
        top1.order_channel
    from cleaned s
    left join lateral (
            select c.status as order_status, c.channel as order_channel
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_latest s
    where exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and c.units between 19 and 39
        )
),
with_latest_stores as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.sale_date,
        latest.store_sale_revenue
    from kept s
    left join lateral (
            select c.sale_date, c.revenue as store_sale_revenue
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) latest on true
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        s.region,
        s.order_channel,
        s.sale_date,
        s.store_sale_revenue,
        top1.revenue,
        top1.store_sale_sale_date
    from with_latest_stores s
    left join lateral (
            select c.revenue, c.sale_date as store_sale_sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) top1 on true
)
-- Multi-level aggregation
select
    case when revenue >= 3600 then 'LARGE' else 'SMALL' end as revenue_tier,
    date_trunc('year', sale_date) as year_start,
    approx_percentile(revenue, array(0.25, 0.5, 0.75)) as revenue_quartiles,
    sort_array(collect_set(region)) as region_list,
    avg(revenue) as avg_revenue
from with_last
group by case when revenue >= 3600 then 'LARGE' else 'SMALL' end, date_trunc('year', sale_date)
having avg(revenue) < 1000000
order by year_start desc;
