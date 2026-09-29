select distinct device
from sessions
where browser = 'Firefox'
order by device
limit 20
