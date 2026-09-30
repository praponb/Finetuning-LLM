select quarter(c.signup_date) as signup_quarter, concat_ws(', ', sort_array(collect_set(a.account_type))) as account_type_values, min(a.opened_date) as min_opened_date
from accounts a
left join customers c on a.customer_id = c.id
group by quarter(c.signup_date);
