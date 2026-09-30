SELECT start_date
FROM subscriptions
UNION
SELECT opened_date
FROM stores
WHERE UPPER(TRIM(store_name)) = 'DOWNTOWN EXPRESS'
