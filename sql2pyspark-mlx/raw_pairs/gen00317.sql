SELECT s.*
FROM accounts s
WHERE EXISTS (
        SELECT 1
        FROM transactions x
        WHERE x.account_id = s.id
          AND x.is_flagged = FALSE
    );
