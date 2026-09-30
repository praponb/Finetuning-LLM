SELECT *
FROM (
    SELECT
        city,
        segment,
        credit_limit
    FROM customers
) src
PIVOT (
    AVG(credit_limit)
    FOR segment IN ('Consumer' AS consumer, 'SMB' AS smb)
);
