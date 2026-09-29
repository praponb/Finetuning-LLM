with flattened as (
    select
        id,
        service,
        status_code,
        key,
        v
    from app_logs
    lateral view outer explode(context) li as key, v
)
select
    id,
    service,
    cume_dist() over (order by status_code desc) as status_code_cume_dist,
    dense_rank() over (partition by service order by status_code desc) as status_code_dense_rank,
    dense_rank() over (order by status_code desc) as status_code_dense_rank_2,
    round(status_code / sum(status_code) over (partition by service, key), 4) as status_code_share
from flattened
