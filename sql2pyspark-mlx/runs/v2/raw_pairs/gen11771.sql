select browser
from sessions
where to_date(started_at) < '2025-07-01'
union
select browser
from sessions
where browser = 'Firefox';
