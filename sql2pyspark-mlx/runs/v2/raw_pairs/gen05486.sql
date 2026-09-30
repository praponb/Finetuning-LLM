select device
from sessions
where device = 'desktop'
intersect
select device
from sessions
where country = 'TH';
