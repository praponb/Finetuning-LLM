select s.id, s.country, s.started_at from sessions s where s.country not in ('SG', 'UK') and not exists (select 1 from web_events we where we.session_id = s.id and we.duration_sec > 134);
