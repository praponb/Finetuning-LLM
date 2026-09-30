select id, region, store_name from stores where city = 'London' except select id, region, store_name from stores where city not in ('New York', 'Chiang Mai', 'Bangkok') order by region desc;
