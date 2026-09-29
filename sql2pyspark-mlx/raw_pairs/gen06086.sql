SELECT * FROM transactions QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS LAST, id ASC) = 1;
