select id, amount, txn_time
from transactions
where amount > (select avg(amount) from transactions);
