select city from suppliers union select city from stores where month(opened_date) in (8, 9, 10) order by city
