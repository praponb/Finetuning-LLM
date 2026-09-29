with lines as (
    select
        id,
        service,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) x as k, value
)
select
    id,
    status_code,
    k,
    round(status_code / sum(status_code) over (partition by service), 4) as status_code_share
from lines
where k = 'version'
