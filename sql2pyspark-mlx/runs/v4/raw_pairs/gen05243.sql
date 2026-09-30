SELECT
    *,
    LEAD(balance, 1, 0) OVER (PARTITION BY status ORDER BY opened_date, id) next_balance,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) event_seq,
    ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date DESC, id) row_seq,
    LAST_VALUE(status, TRUE) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status,
    MAX(balance) OVER (PARTITION BY account_type, status) account_type_max_balance
FROM accounts;
