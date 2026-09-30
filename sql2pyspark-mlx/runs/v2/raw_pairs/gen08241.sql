select id, account_id, merchant from transactions where amount < 3700 and amount is null order by merchant desc, account_id desc
