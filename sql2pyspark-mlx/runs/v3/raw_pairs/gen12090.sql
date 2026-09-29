select status, count(*) as total_count from accounts where not (status not in ('OPEN', 'CLOSED', 'FROZEN') and account_type = 'CHECKING') group by status having count(*) >= 5
