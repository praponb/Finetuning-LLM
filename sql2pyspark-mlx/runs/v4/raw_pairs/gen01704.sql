with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from sessions s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.session_id in (3, 9, 11)
        )
)
-- Multi-level aggregation
select
    browser,
    date_trunc('week', started_at) as week_start,
    count(*) filter (where browser = 'Safari') as safari_rows,
    sort_array(collect_set(country)) as country_list,
    count(case when device = 'tablet' then 1 end) as tablet_count,
    count(distinct country) as distinct_countries
from eligible
group by browser, date_trunc('week', started_at)
having count(case when device = 'tablet' then 1 end) > 1
order by safari_rows desc
