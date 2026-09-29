select t.id, t.is_flagged from transactions t where t.is_flagged = false and t.account_id not in (select id from accounts where status in ('FROZEN', 'CLOSED')) order by t.id limit 3
