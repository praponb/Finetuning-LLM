select id, customer_id, balance, opened_date from (select id, customer_id, balance, opened_date, rank() over (partition by customer_id order by balance desc) as pos from accounts) as r where pos = 1;
