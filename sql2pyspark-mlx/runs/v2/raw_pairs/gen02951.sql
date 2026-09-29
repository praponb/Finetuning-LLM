with store_totals as (
    select store_id, count(units) as units_count
    from store_sales
    where product_id in (4, 7, 8)
    group by store_id
)
select s.store_name, s.opened_date, st.units_count
from stores s
inner join store_totals st on s.id = st.store_id
where st.units_count >= 10
order by opened_date desc
