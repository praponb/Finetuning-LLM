SELECT * FROM (SELECT CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, country, rating FROM suppliers)
PIVOT (SUM(rating) FOR country IN ('JP', 'UK', 'US', 'TH'));
