with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        level,
        status_code,
        payload_json,
        coalesce(context['region'], 'direct') as region_or_default,
        shiftright(status_code, 2) as status_code_high,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
    where level <> 'ERROR'
)
-- Aggregate with conditional metrics
select
    case when status_code_high >= 2 then 'LARGE' else 'SMALL' end as status_code_high_tier,
    round(sum(duration_sec) / nullif(count(case when level = 'DEBUG' then 1 end), 0), 2) as duration_sec_per_debug,
    max(status_code) filter (where level = 'WARN') as warn_max_status_code
from flattened_attrs
group by status_code_high_tier
order by status_code_high_tier desc
