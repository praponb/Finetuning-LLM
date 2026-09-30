select *
from customers
where month(signup_date) in (4, 8, 9) and city = 'New York' and signup_date >= '2023-04-02'
order by country;
