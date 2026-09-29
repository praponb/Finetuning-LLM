with store_totals as (
    select store_id, round(stddev(revenue), 2) as stddev_revenue, round(avg(units), 2) as avg_units
    from store_sales
    group by store_id
)
select
    s.id,
    s.region,
    s.store_name,
    st.stddev_revenue,
    st.avg_units
from stores s
inner join store_totals st on s.id = st.store_id
where st.avg_units > 62
order by st.stddev_revenue desc, id;
