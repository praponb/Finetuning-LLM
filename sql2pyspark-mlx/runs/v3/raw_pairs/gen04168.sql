SELECT * FROM (SELECT QUARTER(signup_date) AS signup_quarter, segment, age FROM customers) PIVOT (MAX(age) FOR segment IN ('Consumer', 'SMB', 'Corporate'))
