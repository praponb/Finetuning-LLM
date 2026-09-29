SELECT
    id,
    signup_date,
    is_active,
    city,
    WEEKOFYEAR(signup_date) AS signup_date_weekofyear,
    TRIM(name) AS clean_name
FROM customers
WHERE segment IN ('SMB', 'Corporate') AND age <> 53
