SELECT
    id,
    country,
    city
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS LAST, id) AS rank_in_group
    FROM suppliers
) t
WHERE rank_in_group = 1
