with store_totals as (
    select store_id, max(discount) as max_discount, round(avg(discount), 2) as avg_discount
    from orders
    group by store_id
)
select s.opened_date, st.max_discount, st.avg_discount
from stores s
left join store_totals st on s.id = st.store_id
where st.avg_discount >= 70
order by st.avg_discount;
