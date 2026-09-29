select s.country, count(case when we.page = '/search' then 1 end) as search_count from web_events we left join sessions s on we.session_id = s.id group by s.country having search_count > 5
