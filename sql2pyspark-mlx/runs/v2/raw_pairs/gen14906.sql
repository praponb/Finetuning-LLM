SELECT id, account_id, amount, is_flagged FROM (SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount) rnk FROM transactions) ranked WHERE rnk <= 5 ORDER BY amount DESC;
