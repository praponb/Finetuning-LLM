select amount, round(amount * 1.1, 2) as amount_adjusted from transactions where txn_type in ('FEE', 'CREDIT');
