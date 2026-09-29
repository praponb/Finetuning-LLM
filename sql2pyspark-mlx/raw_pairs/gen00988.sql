SELECT country FROM customers EXCEPT SELECT country FROM sessions WHERE country IN ('JP', 'TH')
