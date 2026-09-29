select t.is_flagged, a.balance, a.customer_id, c.credit_limit from transactions t left join accounts a on a.id = t.account_id full outer join customers c on a.customer_id = c.id;
