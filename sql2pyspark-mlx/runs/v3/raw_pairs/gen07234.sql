with supplier_totals as (
    select supplier_id, min(cost) as min_cost
    from products
    group by supplier_id
)
select
    s.id,
    s.country,
    s.supplier_name,
    st.min_cost
from suppliers s
inner join supplier_totals st on s.id = st.supplier_id
order by supplier_name, id
limit 10
