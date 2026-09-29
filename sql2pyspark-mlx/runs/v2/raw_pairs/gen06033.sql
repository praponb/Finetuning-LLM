select * from customers where not (segment = 'Corporate' and age between 39 and 47) order by city, id limit 20;
