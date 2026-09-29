select id, balance, account_type
from accounts
where balance between 56000 and 76000
order by id desc, balance asc;
