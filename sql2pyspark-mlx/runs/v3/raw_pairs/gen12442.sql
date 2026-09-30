select city, is_active, min(credit_limit) as min_credit_limit from customers where city = 'Chiang Mai' and upper(email) like '%AVI%' group by city, is_active having min_credit_limit >= 7400;
