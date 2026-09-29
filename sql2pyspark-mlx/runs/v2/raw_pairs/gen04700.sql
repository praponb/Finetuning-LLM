SELECT merchant, COUNT(*) AS total_count
FROM transactions
WHERE is_flagged
GROUP BY merchant;
