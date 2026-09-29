select browser
from sessions
where country = 'JP'
union all
select browser
from sessions
where browser <> 'Chrome';
