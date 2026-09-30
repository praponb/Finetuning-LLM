select * from (select city, max(age) - min(age) as age_range from customers group by city) as agg where age_range >= 75
