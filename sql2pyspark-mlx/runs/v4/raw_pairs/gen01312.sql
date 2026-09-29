with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from products s
    where exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.order_id in (5, 11, 25, 32)
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date >= s.launch_date
        )
),
with_dims as (
    -- Attach suppliers attributes
    select e.*, s.rating
    from not_flagged e
    left join suppliers s
        on e.supplier_id = s.id
)
-- Aggregate with conditional metrics
select
    case when cost >= 730 then 'LARGE' else 'SMALL' end as cost_tier,
    min(cost) as min_cost,
    round(sum(cost) / nullif(count(case when category = 'Home' then 1 end), 0), 2) as cost_per_home,
    count_if(brand = 'Initech') as initech_events,
    count(*) filter (where brand = 'Globex') as globex_rows
from with_dims
group by case when cost >= 730 then 'LARGE' else 'SMALL' end
