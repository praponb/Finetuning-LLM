select
    id,
    customer_id,
    concat_ws('/', status, account_type) as status_account_type_key,
    length(account_type) as account_type_length
from accounts
