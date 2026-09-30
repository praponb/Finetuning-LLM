SELECT promo_code, id
FROM promotions
INTERSECT ALL
SELECT promo_code, id
FROM promotions
WHERE ends_on BETWEEN '2023-10-01' AND '2024-10-01';
