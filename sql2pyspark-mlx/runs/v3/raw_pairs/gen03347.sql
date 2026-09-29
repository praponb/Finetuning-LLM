select *
from (
    select store_id, case when revenue >= 3500 then 'high' when revenue >= 800 then 'mid' else 'low' end revenue_bucket, max(units) max_units
    from store_sales
    group by store_id, case when revenue >= 3500 then 'high' when revenue >= 800 then 'mid' else 'low' end
) agg
where max_units > 17;
