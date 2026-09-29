select we.id, we.duration_sec from web_events we where we.duration_sec >= 403 and we.session_id not in (select id from sessions where started_at >= '2024-07-25') order by we.duration_sec desc;
