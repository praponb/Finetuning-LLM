select credit_limit, email, case when is_active then 'Yes' else 'No' end as is_active_label
from customers;
