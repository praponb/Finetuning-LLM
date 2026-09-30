select accounts.id, accounts.account_type, count(transactions.id) transaction_count
from accounts
join transactions on accounts.id = transactions.account_id
group by accounts.id, accounts.account_type
order by accounts.account_type, id desc
