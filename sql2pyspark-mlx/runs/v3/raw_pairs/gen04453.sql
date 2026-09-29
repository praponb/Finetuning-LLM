with numbered as (select id, customer_id, opened_date, balance, row_number() over (partition by customer_id order by opened_date desc, id) as rn from accounts) select * from numbered where rn = 1
