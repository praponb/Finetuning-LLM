select *, hour(started_at) as started_at_hour
from sessions
where country = 'TH' and started_at < '2024-08-01'
