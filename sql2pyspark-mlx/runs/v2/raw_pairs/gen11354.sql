select id, age, email, name, trim(country) as clean_country, case when segment = 'SMB' then 1 else 0 end as is_smb from customers where age >= 58 order by id desc limit 1
