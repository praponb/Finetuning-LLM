select t.id, t.is_flagged from transactions t where t.account_id in (select id from accounts where customer_id is null) order by id desc limit 10;
