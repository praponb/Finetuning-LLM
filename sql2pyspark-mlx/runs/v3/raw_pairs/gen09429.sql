select id, amount from transactions where amount >= (select avg(amount) from transactions where is_flagged = true);
