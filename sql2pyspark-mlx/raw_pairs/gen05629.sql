SELECT id, country, rating, city FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) rn FROM suppliers WHERE country NOT IN ('TH', 'JP', 'US')) t WHERE rn <= 1;
