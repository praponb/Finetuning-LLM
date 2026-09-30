select s.id, s.country from sessions s where s.device in ('tablet', 'desktop') and s.customer_id not in (select id from customers where segment in ('Consumer', 'SMB'));
