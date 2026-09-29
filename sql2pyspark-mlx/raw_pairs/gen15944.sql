select
    email,
    name,
    credit_limit,
    concat_ws(',', name, email) name_email_key
from customers
where credit_limit between 4800 and 13300 and (country in ('DE', 'JP', 'US') or city in ('Tokyo', 'Berlin', 'Bangkok'));
