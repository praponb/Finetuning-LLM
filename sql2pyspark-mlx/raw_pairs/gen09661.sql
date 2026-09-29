select t.id, t.is_flagged, a.opened_date from transactions t right join accounts a on t.account_id = a.id where t.amount >= 2400
