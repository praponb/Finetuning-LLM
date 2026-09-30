select id, name from customers where city not in ('Berlin', 'Tokyo') and segment != 'Corporate' order by id;
