SELECT status FROM accounts INTERSECT ALL SELECT status FROM accounts WHERE status = 'FROZEN'
