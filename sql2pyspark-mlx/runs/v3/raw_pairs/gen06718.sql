SELECT id, balance, ROW_NUMBER() OVER (ORDER BY balance DESC, id) AS rn FROM accounts ORDER BY rn DESC
