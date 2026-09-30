with latest as (
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by sale_date desc, id) as rank_in_group
        from store_sales
    ) x
    where rank_in_group = 1
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.revenue,
    s.rank_in_group,
    (select max(o2.quantity) from order_items o2 where o2.product_id = s.product_id) as max_order_item_quantity
from latest s
order by store_id desc nulls last;
