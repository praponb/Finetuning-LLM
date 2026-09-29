select
    customer_id,
    account_type,
    coalesce(status, 'N/A') as status_filled,
    last_day(opened_date) as opened_date_month_end
from accounts
where account_type in ('CREDIT', 'LOAN', 'SAVINGS')
