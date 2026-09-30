SELECT
    id,
    credit_limit,
    COALESCE(segment, 'N/A') AS segment_filled,
    ADD_MONTHS(signup_date, 1) AS signup_date_plus_months
FROM customers
WHERE LENGTH(name) < 5
