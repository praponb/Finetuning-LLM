select we.id, we.page, we.session_id from web_events we where we.session_id in (select id from sessions where customer_id = 26) order by we.session_id desc, page, id limit 5;
