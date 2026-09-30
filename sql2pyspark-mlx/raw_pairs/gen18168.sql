WITH filtered AS (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE end_date >= DATE '2023-06-26'
)
SELECT
    d.segment,
    a.status,
    COUNT(*) AS cnt,
    AVG(a.monthly_fee) AS avg_monthly_fee,
    GROUPING_ID() AS gid
FROM filtered a
INNER JOIN customers d ON a.customer_id = d.id
GROUP BY CUBE (d.segment, a.status)
