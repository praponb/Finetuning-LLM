select
    s.country,
    date_trunc('MONTH', s.started_at) as started_month,
    round(stddev(we.duration_sec), 2) as stddev_duration_sec,
    count(distinct we.duration_sec) as unique_pages
from web_events we
left join sessions s on we.session_id = s.id
group by s.country, date_trunc('MONTH', s.started_at)
order by country desc, started_month asc
limit 1
