with parsed as (
    select
        id,
        host,
        level,
        status_code,
        payload_json,
        context,
        get_json_object(payload_json, '$.user.id') as user_id,
        array_contains(map_keys(context), 'trace') as has_trace,
        size(context) as num_context
    from app_logs
)
select
    *,
    lead(status_code) over w_grp next_status_code,
    lag(num_context) over w_grp prev_num_context,
    row_number() over w_grp row_seq,
    first_value(num_context, true) over (partition by host, level order by num_context, id rows between unbounded preceding and current row) first_known_num_context
from parsed
window w_grp as (partition by level order by num_context, id)
