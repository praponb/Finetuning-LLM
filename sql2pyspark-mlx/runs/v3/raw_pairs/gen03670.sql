select id, merchant
from transactions
where amount >= 1000
union
select id, merchant
from transactions
where amount > 1300
order by id desc;
