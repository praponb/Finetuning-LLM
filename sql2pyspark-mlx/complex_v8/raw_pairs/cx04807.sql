SELECT id AS ref_id, username AS label
FROM listeners
EXCEPT
SELECT id AS ref_id, country AS label
FROM listeners
WHERE signup_date >= DATE '2024-08-02'
