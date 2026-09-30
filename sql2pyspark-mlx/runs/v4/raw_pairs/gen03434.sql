with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        ceil(unit_price) as unit_price_whole,
        case when quantity >= 6 then 'High' when quantity >= 4 then 'Medium' else 'Low' end as quantity_tier
    from order_items
    where quantity <= 6
)
-- Grouped metrics
select
    product_id,
    count(distinct order_id) as distinct_order_ids,
    count(*) as cnt
from base
group by product_id
having count(*) >= 1
order by product_id;
