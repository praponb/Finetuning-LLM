with matched_keys as (
    -- Set difference on keys
    select device, browser
    from sessions
    except all
    select device, browser
    from sessions
    where browser not in ('Chrome', 'Firefox', 'Safari')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions s
    join matched_keys k on s.device = k.device and s.browser = k.browser
)
-- Multi-level aggregation
select
    date_trunc('week', started_at) as week_start,
    count(*) filter (where country = 'DE') as de_rows,
    count(case when country = 'TH' then 1 end) as th_count,
    max(started_at) as last_started_at
from verified
group by week_start
having count(case when country = 'TH' then 1 end) > 1;
