select brand, unique_brands from (select brand, count(distinct cost) as unique_brands from products group by brand) as agg where unique_brands < 4 order by unique_brands;
