select status from accounts where opened_date < '2025-03-23' union all select status from accounts where account_type in ('CHECKING', 'SAVINGS', 'LOAN') order by status
