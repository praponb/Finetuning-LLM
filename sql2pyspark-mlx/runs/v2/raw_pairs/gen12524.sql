SELECT a.id, a.opened_date FROM accounts a WHERE a.account_type IN ('LOAN', 'CREDIT', 'CHECKING') AND a.id IN (SELECT account_id FROM transactions) ORDER BY opened_date DESC, id DESC;
