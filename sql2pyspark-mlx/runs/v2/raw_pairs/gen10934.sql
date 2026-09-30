select *, length(status) as status_length from accounts where (account_type not in ('CHECKING', 'SAVINGS', 'LOAN') and account_type != 'SAVINGS') or opened_date between '2024-04-01' and '2024-10-05'
