select
    date_trunc('month', started_at) as activity_month,
    browser,
    date_trunc('quarter', started_at) as quarter_start,
    count(*) filter (where browser = 'Firefox') as firefox_rows,
    size(collect_set(country)) as num_countries,
    count(case when country = 'US' then 1 end) as us_count
from sessions
group by date_trunc('month', started_at), browser, date_trunc('quarter', started_at)
having count(*) filter (where browser = 'Firefox') >= 1
