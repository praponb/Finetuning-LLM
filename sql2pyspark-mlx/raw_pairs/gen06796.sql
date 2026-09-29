WITH AccountsByCustomer AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
    FROM accounts
    WHERE opened_date >= '2023-10-01'
    GROUP BY customer_id
)
SELECT
    customers.name,
    customers.email,
    AccountsByCustomer.status_values,
    AccountsByCustomer.account_type_values
FROM customers
INNER JOIN AccountsByCustomer ON customers.id = AccountsByCustomer.customer_id
ORDER BY status_values;
