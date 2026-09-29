select *, concat(country, ', ', browser) as country_browser, cast(started_at as date) as started_at_day
from sessions
where started_at >= '2023-12-01' or browser in ('Firefox', 'Safari');
