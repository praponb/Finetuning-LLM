SELECT method, status, paid_at, CEIL(amount) amount_whole FROM payments ORDER BY paid_at;
