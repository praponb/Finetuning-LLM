select a.opened_date, a.customer_id, t.txn_time, t.is_flagged, trim(t.merchant) as clean_merchant from accounts a left join transactions t on a.id = t.account_id order by clean_merchant
