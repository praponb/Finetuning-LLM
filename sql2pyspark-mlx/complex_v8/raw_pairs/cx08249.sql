SELECT
    'holder' AS origin,
    segment,
    holder_name AS name,
    joined_on
FROM policyholders
UNION
SELECT
    'policy' AS origin,
    status AS segment,
    product AS name,
    end_date AS joined_on
FROM policies
ORDER BY origin DESC, name
