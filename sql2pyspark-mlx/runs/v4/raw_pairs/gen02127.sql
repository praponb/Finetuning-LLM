WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY age, id) AS rank_in_group
        FROM customers
    ) t
    WHERE rank_in_group = 1
)
SELECT
    id,
    name,
    city,
    segment,
    is_active,
    credit_limit,
    rank_in_group,
    CASE WHEN segment RLIKE '^(SMB|Corporate)$' THEN 'CORE' WHEN segment RLIKE '^(Consumer)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS segment_zone
FROM deduped;
