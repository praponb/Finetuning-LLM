select
    method,
    status,
    amount,
    length(method) as method_length
from payments
where hour(paid_at) between 9 and 12 and method = 'bank_transfer';
