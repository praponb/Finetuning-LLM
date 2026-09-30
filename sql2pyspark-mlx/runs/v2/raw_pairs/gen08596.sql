select credit_limit, initcap(trim(name)) as name_title, floor(credit_limit) as credit_limit_whole from customers where country != 'JP'
