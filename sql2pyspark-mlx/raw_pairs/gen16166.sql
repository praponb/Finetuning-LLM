SELECT *
FROM (
    SELECT CASE WHEN country IN ('TH', 'SG') THEN 'tier_1' ELSE 'other' END AS country_group, AVG(rating) AS avg_rating
    FROM suppliers
    WHERE supplier_name IS NOT NULL
    GROUP BY CASE WHEN country IN ('TH', 'SG') THEN 'tier_1' ELSE 'other' END
) AS agg
WHERE avg_rating < 4.1;
