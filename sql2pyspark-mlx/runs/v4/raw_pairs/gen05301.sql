SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rank_in_group
    FROM suppliers
) t
WHERE rank_in_group = 1
