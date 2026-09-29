select
    page,
    count(duration_sec) as duration_sec_count,
    sort_array(collect_set(page)) as page_list,
    sum(case when event_type = 'click' then 1 else 0 end) as click_count
from web_events
where duration_sec is null or session_id in (9, 10)
group by page
having duration_sec_count < 3
order by click_count, page
limit 1
