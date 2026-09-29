select s.device, sort_array(collect_set(we.page)) as page_list from web_events we inner join sessions s on we.session_id = s.id group by s.device;
