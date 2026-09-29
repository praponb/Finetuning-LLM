select *
from transactions
where merchant = '7-Eleven' and is_flagged = false
order by id desc;
