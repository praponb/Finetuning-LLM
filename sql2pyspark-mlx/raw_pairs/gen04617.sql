WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY txn_time DESC NULLS LAST, id) AS rank_in_group
        FROM transactions
    ) t
    WHERE rank_in_group = 1
)
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    STDDEV_SAMP(amount) AS amount_stddev,
    ROUND(AVG(amount), 2) AS avg_amount,
    STDDEV_SAMP(amount) AS amount_stddev_2,
    STDDEV_SAMP(amount) AS amount_stddev_3
FROM ranked
GROUP BY DATE_TRUNC('WEEK', txn_time), DATE_TRUNC('MONTH', txn_time);
