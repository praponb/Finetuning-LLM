select id, amount from transactions where amount > (select avg(amount) from transactions where account_id in (7, 12, 20, 34));
