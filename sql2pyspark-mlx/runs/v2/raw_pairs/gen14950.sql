select page, sum(case when page = '/home' then 1 else 0 end) as home_count from web_events group by page having home_count < 64 order by home_count desc;
