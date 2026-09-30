with flattened as (
    select
        id,
        service,
        host,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) li as attr_key, v
)
select
    attr_key,
    size(collect_set(service)) as num_services,
    sort_array(collect_set(attr_key)) as attr_key_list,
    count(*) filter (where attr_key = 'trace') as trace_rows,
    count_if(host = 'h2') as h2_events,
    count(distinct service) as distinct_services
from flattened
group by attr_key
order by num_services desc nulls last;
