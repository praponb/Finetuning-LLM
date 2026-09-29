select
    status,
    customer_id,
    balance,
    add_months(opened_date, 6) as opened_date_plus_months
from accounts
where account_type <> 'CHECKING' and opened_date <= '2024-12-16'
order by status asc;
