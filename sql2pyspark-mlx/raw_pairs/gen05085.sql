SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS LAST, id ASC) AS rank_in_group
    FROM transactions
) ranked
WHERE rank_in_group = 1;
