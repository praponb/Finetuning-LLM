SELECT opened_date FROM stores WHERE city NOT IN ('London', 'Bangkok', 'New York') EXCEPT SELECT opened_date FROM stores WHERE YEAR(opened_date) = 2024
