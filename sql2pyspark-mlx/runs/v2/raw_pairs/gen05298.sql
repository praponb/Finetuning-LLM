select id, browser, cast(started_at as date) as started_at_day
from sessions
where country = 'JP'
