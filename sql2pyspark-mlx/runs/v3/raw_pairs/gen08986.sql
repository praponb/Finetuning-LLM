with StoreStats as (
    select store_id, count(distinct units) as unique_products
    from store_sales
    group by store_id
)
select s.id, s.opened_date, S.unique_products
from stores s
left outer join StoreStats S on s.id = S.store_id
where S.unique_products >= 7
order by s.id desc
