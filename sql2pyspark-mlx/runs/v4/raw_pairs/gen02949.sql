select id, device
from sessions
where started_at >= '2023-11-09'
order by id
limit 1
