select t.id, t.txn_type, t.amount
from transactions t
where t.account_id not in (select id from accounts where customer_id in (7, 10, 19, 31))
