select accounts.id, accounts.status, count(transactions.id) as transaction_count
from accounts
left outer join transactions on accounts.id = transactions.account_id
group by accounts.id, accounts.status;
