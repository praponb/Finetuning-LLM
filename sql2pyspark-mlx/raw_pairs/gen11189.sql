with ranked as (
    select *, rank() over (partition by brand order by price desc nulls first) as dedup_rank
    from products
    qualify dedup_rank = 1
),
not_flagged as (
    select s.*
    from ranked s
    where exists (
            select 1
            from inventory ch
            where ch.product_id = s.id
              and ch.last_restock < s.launch_date
        )
),
joined as (
    select
        e.id,
        e.product_name,
        e.category,
        e.brand,
        e.price,
        e.stock_qty,
        e.supplier_id,
        s.country,
        s.city,
        s.rating
    from not_flagged e
    inner join suppliers s
        on e.supplier_id = s.id
    where s.rating >= 3.0
),
with_latest as (
    select
        s.id,
        s.brand,
        s.price,
        s.supplier_id,
        s.city,
        s.rating,
        latest.store_sale_revenue
    from joined s
    left join lateral (
            select c.revenue as store_sale_revenue
            from store_sales c
            where c.product_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) latest on true
)
select
    s.id,
    s.price,
    s.supplier_id,
    s.city,
    s.rating,
    s.store_sale_revenue,
    case when city rlike '^(Bangkok|Chiang Mai)$' then 'EXTENDED' else 'GROUP_A' end as city_zone,
    (select sum(p2.price) from products p2 where p2.supplier_id = s.supplier_id) as sum_product_price
from with_latest s
order by id, sum_product_price
limit 20;
