with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
),
cleaned as (
    -- Keep recent, valid rows
    select *, upper(host) host_upper
    from lines
    where attr_key not in ('version', 'region')
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        *,
        substring(host, 1, 4) host_prefix,
        coalesce(attr_key, 'unassigned') attr_key_filled
    from cleaned
    where attr_key is not null
),
remaining_keys as (
    -- Set difference on keys
    select host, id
    from cleaned_app_logs
    except all
    select host, id
    from app_logs
    where hour(log_ts) between 6 and 11
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned_app_logs s
    inner join remaining_keys k on s.host = k.host and s.id = k.id
)
select
    attr_key,
    host_upper,
    attr_key_filled
from reconciled
order by attr_key nulls first;
