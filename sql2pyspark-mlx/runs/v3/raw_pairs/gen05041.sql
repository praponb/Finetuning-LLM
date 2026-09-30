SELECT id, account_id, amount, is_flagged FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS row_num FROM transactions) AS ranked WHERE row_num <= 5;
