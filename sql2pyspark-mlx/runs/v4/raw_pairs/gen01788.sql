SELECT country FROM customers WHERE UPPER(email) LIKE '%RAN%' INTERSECT SELECT country FROM sessions;
