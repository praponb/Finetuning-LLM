SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS rn
    FROM subscriptions
) AS r
WHERE rn <= 5
ORDER BY status ASC;
