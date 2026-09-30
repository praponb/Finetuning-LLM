WITH cleaned AS (
    SELECT
        *,
        TRIM(name) AS clean_name,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM customers
    WHERE signup_date > DATE '2023-08-04'
)
SELECT
    id,
    name,
    segment,
    signup_date,
    age,
    clean_name,
    resolved_segment,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share,
    SUM(credit_limit_filled) OVER w_ordered AS cumulative_credit_limit_filled,
    AVG(credit_limit) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS credit_limit_avg_6
FROM cleaned
WINDOW w_ordered AS (PARTITION BY YEAR(signup_date), segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
