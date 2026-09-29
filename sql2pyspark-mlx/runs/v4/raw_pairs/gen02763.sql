SELECT s.id, s.browser, c.segment, c.country, c.age, UPPER(c.segment) AS segment_upper FROM sessions s FULL OUTER JOIN customers c ON c.id = s.customer_id
