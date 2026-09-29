select is_active, stddev_age from (select is_active, round(stddev(age), 2) as stddev_age from customers group by is_active) as sub where stddev_age >= 64 order by stddev_age desc, is_active limit 50
