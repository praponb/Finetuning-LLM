WITH rollup_totals AS (
    SELECT
        c.segment,
        c.city,
        src.status,
        COUNT(*) AS cnt,
        MAX(src.balance) AS max_balance
    FROM accounts src
    INNER JOIN customers c ON src.customer_id = c.id
    GROUP BY GROUPING SETS ((c.segment, c.city, src.status), (c.segment))
)
SELECT
    segment,
    status,
    cnt,
    max_balance,
    CASE WHEN segment RLIKE '^(Corporate|Consumer)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS segment_zone
FROM rollup_totals
ORDER BY max_balance DESC NULLS LAST, cnt;
