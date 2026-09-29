select *, upper(status) as status_upper, coalesce(monthly_fee, 0) as monthly_fee_filled from subscriptions where monthly_fee > 200 or end_date <= '2024-02-01';
