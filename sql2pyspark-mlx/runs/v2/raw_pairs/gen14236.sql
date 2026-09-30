SELECT
    id,
    balance,
    WEEKOFYEAR(opened_date) AS opened_date_weekofyear,
    CONCAT_WS(',', account_type, status) AS account_type_status_key
FROM accounts;
