select id, email, age + credit_limit as age_plus_credit_limit from customers where credit_limit between 11700 and 15500 and email like '%@startup.io' and name like '%Kanya%';
