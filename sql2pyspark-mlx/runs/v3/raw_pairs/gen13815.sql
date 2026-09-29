with filtered_order_items as (
    select *, greatest(quantity, unit_price) as greatest_quantity_unit_price
    from order_items
    where unit_price > 230 and order_id in (9, 23, 38)
),
greatest_quantity_unit_price_bucket_summary as (
    select case when greatest_quantity_unit_price >= 66 then 'high' when greatest_quantity_unit_price >= 36 then 'mid' else 'low' end as greatest_quantity_unit_price_bucket, avg(greatest_quantity_unit_price) as avg_greatest_quantity_unit_price
    from filtered_order_items
    group by case when greatest_quantity_unit_price >= 66 then 'high' when greatest_quantity_unit_price >= 36 then 'mid' else 'low' end
)
select *
from greatest_quantity_unit_price_bucket_summary
where avg_greatest_quantity_unit_price < 40
order by greatest_quantity_unit_price_bucket desc
