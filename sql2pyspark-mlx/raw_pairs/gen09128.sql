select city, round(stddev(age), 2) as stddev_age, round(avg(age), 2) as avg_age from customers group by 1
