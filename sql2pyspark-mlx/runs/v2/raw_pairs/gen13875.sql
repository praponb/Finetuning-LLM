select id, store_id, last_day(last_restock) last_restock_month_end
from inventory
where quantity = 166
order by id desc
limit 1
