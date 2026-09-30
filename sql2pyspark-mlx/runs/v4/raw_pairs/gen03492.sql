select id, customer_id, balance from (select id, customer_id, balance, row_number() over (partition by customer_id order by balance desc, id) rn from accounts) t where rn <= 5 order by balance;
