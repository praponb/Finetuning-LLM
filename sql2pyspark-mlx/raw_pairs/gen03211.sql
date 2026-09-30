select city, country, round(stddev(credit_limit), 2) as stddev_credit_limit from customers group by city, country having stddev_credit_limit >= 37
