select s.id, s.opened_date, s.store_name
from stores s
where s.id in (select store_id from orders where amount between 860 and 880)
order by id desc
