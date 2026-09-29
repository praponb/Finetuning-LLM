select we.id, we.page, s.browser, s.device, s.customer_id from web_events we inner join sessions s on we.session_id = s.id order by page;
