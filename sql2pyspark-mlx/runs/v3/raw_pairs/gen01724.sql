select city from customers union all select city from stores where length(store_name) > 8 order by city
