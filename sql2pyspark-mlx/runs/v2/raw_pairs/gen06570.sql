SELECT id, status, DATE_FORMAT(end_date, 'yyyy-MM-dd') end_date_label, COALESCE(monthly_fee, 0) monthly_fee_filled FROM subscriptions WHERE monthly_fee IS NULL;
