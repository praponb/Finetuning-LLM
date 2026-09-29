SELECT
    id,
    store_id,
    amount,
    INITCAP(TRIM(status)) status_title
FROM orders;
