select
    month(event_time) event_month,
    concat_ws(', ', sort_array(collect_set(page))) page_values,
    count(case when page = '/cart' then 1 end) cart_count,
    count(case when page = '/checkout' then 1 end) checkout_count
from web_events
group by month(event_time)
order by checkout_count desc, event_month desc
limit 10;
