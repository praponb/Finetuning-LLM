with latest_match as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.region,
        s.opened_date,
        top1.store_sale_revenue,
        top1.units
    from stores s
    join lateral (
            select c.revenue as store_sale_revenue, c.units
            from store_sales c
            where c.store_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) top1 on true
)
-- Aggregate with conditional metrics
select
    region,
    count(case when region = 'West' then 1 end) as west_count,
    count(distinct id) as distinct_ids,
    count(case when region = 'Central' then 1 end) as central_count
from latest_match
group by region
