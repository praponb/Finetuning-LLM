select account_id, concat_ws('-', txn_type, merchant) as txn_type_merchant_key, ceil(amount) as amount_whole
from transactions
order by amount_whole;
