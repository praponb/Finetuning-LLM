select * from (select date_format(opened_date, 'yyyy-MM') as opened_month, account_type, balance from accounts)
pivot (count(balance) for account_type in ('CHECKING', 'LOAN'));
