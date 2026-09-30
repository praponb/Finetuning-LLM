with not_flagged as (
    select s.*
    from products s
    where not exists (
            select 1
            from inventory c
            where c.product_id = s.id
              and c.last_restock >= s.launch_date
        )
)
select
    case when cost >= 790 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    category,
    sort_array(collect_set(category)) as category_list,
    sum(stock_qty) filter (where category = 'Grocery') as grocery_total_stock_qty,
    max(cost) as max_cost,
    sort_array(collect_set(brand)) as brand_list
from not_flagged
group by case when cost >= 790 then 'PREMIUM' else 'STANDARD' end, category
having grocery_total_stock_qty > 0
order by max_cost nulls last
