select account_type, customer_id, case when balance >= 79000 then 'High' when balance >= 31000 then 'Medium' else 'Low' end as balance_level from accounts;
