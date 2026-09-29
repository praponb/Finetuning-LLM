select round((100 * sum(case when event_type = 'add_to_cart' then 1 else 0 end)) / count(*), 1) pct_add_to_cart, sort_array(collect_set(page)) page_list from web_events where duration_sec is null;
