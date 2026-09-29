select *
from payments
where amount between 1260 and 1490 and method is not null
order by id desc
limit 5
