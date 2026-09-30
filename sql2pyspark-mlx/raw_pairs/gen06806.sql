select session_id, concat_ws(', ', sort_array(collect_set(page))) page_values from web_events group by session_id order by session_id desc limit 20
