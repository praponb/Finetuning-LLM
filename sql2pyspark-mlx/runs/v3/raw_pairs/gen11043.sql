select country
from suppliers
where city = 'New York'
intersect
select country
from sessions
order by country desc
