SELECT DISTINCT merchant
FROM transactions
WHERE NOT is_flagged
ORDER BY merchant;
