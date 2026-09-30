select t.id, t.merchant, t.amount from transactions t where t.account_id in (select id from accounts where customer_id is not null) order by amount desc, t.merchant desc
