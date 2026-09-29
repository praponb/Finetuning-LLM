with with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, x.discount
    from stores s
    left join lateral (
            select c.discount
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) x on true
)
-- Grouped metrics
select
    region,
    date_trunc('quarter', opened_date) as quarter_start,
    percentile_approx(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
    size(collect_set(region)) as num_regions,
    round(avg(sqft), 2) as avg_sqft,
    count(case when region = 'East' then 1 end) as east_count,
    count(case when region = 'East' then 1 end) as east_count_2,
    count(*) filter (where region = 'Central') as central_rows
from with_latest
group by grouping sets ((region, date_trunc('quarter', opened_date)), (region), ())
having east_count_2 > 1
order by east_count_2 desc nulls first
