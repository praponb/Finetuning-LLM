with store_totals as (
    select store_id, max(units) - min(units) as units_range, min(sale_date) as first_sale_date
    from store_sales
    where store_id in (1, 3, 4, 6)
    group by store_id
)
select
    s.id,
    s.sqft,
    st.units_range,
    st.first_sale_date
from stores s
left join store_totals st on s.id = st.store_id
where st.units_range > 66
order by id desc;
