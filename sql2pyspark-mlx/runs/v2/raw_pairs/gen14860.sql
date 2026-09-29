SELECT * FROM (SELECT CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, country, rating FROM suppliers)
PIVOT (COUNT(rating) FOR country IN ('UK', 'TH'))
