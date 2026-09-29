SELECT * FROM (SELECT country, city, rating FROM suppliers WHERE country NOT IN ('JP', 'TH', 'SG'))
PIVOT (AVG(rating) FOR city IN ('Singapore', 'Chiang Mai'));
