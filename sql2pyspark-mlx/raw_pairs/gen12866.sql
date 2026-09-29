select t.is_flagged, a.opened_date, c.email from transactions t left join accounts a on t.account_id = a.id join customers c on a.customer_id = c.id order by a.opened_date asc;
