select plan, monthly_fee, cast(monthly_fee as int) monthly_fee_int from subscriptions where start_date <= '2025-03-01' or status in ('ACTIVE', 'EXPIRED');
