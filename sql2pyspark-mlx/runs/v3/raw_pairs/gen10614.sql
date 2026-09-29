SELECT a.balance, t.merchant FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id WHERE a.status IN ('CLOSED', 'FROZEN') AND a.balance < 51000 ORDER BY a.balance ASC, merchant ASC
