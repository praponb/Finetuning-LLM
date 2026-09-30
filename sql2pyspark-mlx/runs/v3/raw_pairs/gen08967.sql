select id, balance, customer_id, account_type, case when status = 'FROZEN' then 1 else 0 end as is_frozen from accounts order by is_frozen desc;
