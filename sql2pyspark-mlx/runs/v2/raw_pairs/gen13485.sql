select id, opened_date from stores where city in ('New York', 'Bangkok') and city in ('Chiang Mai', 'New York') order by opened_date desc, id desc limit 3
