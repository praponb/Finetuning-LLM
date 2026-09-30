select id, opened_date, city from stores where opened_date > '2023-09-01' union all select id, opened_date, city from stores where city = 'Singapore' order by city
