SELECT merchant FROM transactions WHERE merchant IS NOT NULL UNION ALL SELECT country FROM sessions WHERE country IS NOT NULL;
