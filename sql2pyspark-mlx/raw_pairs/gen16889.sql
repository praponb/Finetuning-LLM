SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS seq_no,
    CUME_DIST() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_cume_dist
FROM subscriptions
