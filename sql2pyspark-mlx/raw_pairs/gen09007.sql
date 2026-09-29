SELECT id, balance, ROUND(balance * 100, 2) AS balance_adjusted, ROUND(balance, 1) AS balance_rounded FROM accounts WHERE NOT (balance <= 68000 OR opened_date <= '2025-04-08')
