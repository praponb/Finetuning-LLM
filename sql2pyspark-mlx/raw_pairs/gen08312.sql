select country, id
from sessions
intersect all
select country, id
from sessions
where browser in ('Safari', 'Firefox', 'Chrome')
