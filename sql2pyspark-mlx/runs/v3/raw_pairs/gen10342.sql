select t.id, t.amount, t.is_flagged, t.account_id from transactions t where t.account_id not in (select id from accounts where status not in ('OPEN', 'FROZEN')) order by id;
