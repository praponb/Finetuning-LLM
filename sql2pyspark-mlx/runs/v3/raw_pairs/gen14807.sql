select count(case when account_type = 'LOAN' then 1 end) loan_count, sum(case when status = 'CLOSED' then 1 else 0 end) closed_count, min(opened_date) min_opened_date
from accounts
