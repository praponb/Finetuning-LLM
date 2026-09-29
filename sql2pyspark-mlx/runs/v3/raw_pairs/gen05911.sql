with StoreTotals as (
    select store_id, max(revenue) - min(revenue) as revenue_range
    from store_sales
    group by store_id
)
select s.sqft, s.region, S.revenue_range
from stores s
join StoreTotals S on s.id = S.store_id
where S.revenue_range > 61
order by sqft desc;
