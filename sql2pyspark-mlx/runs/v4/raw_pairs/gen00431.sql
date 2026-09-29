with supplier_totals as (
    select supplier_id, avg(cost) as avg_cost
    from products
    where stock_qty > 207
    group by supplier_id
)
select
    s.id,
    s.supplier_name,
    s.country,
    st.avg_cost
from suppliers s
join supplier_totals st on s.id = st.supplier_id
where st.avg_cost >= 660;
