select t.id, t.txn_type, a.balance
from transactions t
full outer join accounts a on t.account_id = a.id
where t.amount >= 2400 or t.merchant in ('Grab', '7-Eleven', 'Shell');
