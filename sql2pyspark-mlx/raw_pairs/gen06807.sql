select * from (select segment, country, age from customers)
pivot (sum(age) for country in ('DE', 'UK', 'TH', 'US'))
