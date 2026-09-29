select status, cast(balance as int) as balance_int from accounts where status = 'CLOSED' and opened_date between '2025-08-01' and '2025-10-01';
