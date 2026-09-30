with supplier_totals as (
    select supplier_id, min(launch_date) as first_launch_date
    from products
    where supplier_id in (8, 14, 35)
    group by supplier_id
)
select s.rating, st.first_launch_date
from suppliers s
inner join supplier_totals st on s.id = st.supplier_id
