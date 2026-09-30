SELECT signup_date FROM customers UNION SELECT opened_date AS signup_date FROM accounts WHERE opened_date BETWEEN '2023-10-23' AND '2024-10-01' ORDER BY signup_date;
