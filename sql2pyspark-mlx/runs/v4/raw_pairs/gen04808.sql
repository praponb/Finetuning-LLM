SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rank_in_group
    FROM suppliers
) ranked
WHERE rank_in_group = 1
