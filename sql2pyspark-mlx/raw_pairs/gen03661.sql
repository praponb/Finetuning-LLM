with matched_keys as (
    select id, category
    from products
    intersect all
    select id, category
    from products
    where array_contains(tags, 'sale')
),
kept_rows as (
    select s.*
    from products s
    inner join matched_keys k on s.id = k.id and s.category = k.category
)
select
    s.id,
    s.product_name,
    s.category,
    s.price,
    s.cost,
    s.launch_date,
    s.supplier_id,
    case when product_name rlike '^(Backpack XL|Yoga Mat)$' then 'GROUP_A' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'GROUP_B' else 'EXTENDED' end as product_name_zone,
    (select sum(t2.stock_qty) from products t2 where t2.supplier_id = s.supplier_id) as sum_product_stock_qty
from kept_rows s
order by launch_date nulls first;
