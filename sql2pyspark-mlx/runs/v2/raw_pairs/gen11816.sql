select transactions.merchant, transactions.is_flagged, accounts.opened_date
from transactions
inner join accounts on transactions.account_id = accounts.id
order by opened_date desc;
