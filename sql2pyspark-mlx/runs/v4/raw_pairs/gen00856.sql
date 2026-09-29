select id, amount, account_id, merchant from transactions where amount <= (select min(amount) from transactions) order by amount;
