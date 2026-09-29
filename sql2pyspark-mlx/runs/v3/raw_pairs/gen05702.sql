select distinct txn_type, merchant
from transactions
where amount <= 2000;
