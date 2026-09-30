select page, sum(case when page = '/search' then 1 else 0 end) search_count, round(stddev(duration_sec), 2) stddev_duration_sec
from web_events
group by page
having count(duration_sec) < 3;
