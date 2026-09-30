select status from shipments where order_id = 15 union all select status from subscriptions where monthly_fee between 70 and 130;
