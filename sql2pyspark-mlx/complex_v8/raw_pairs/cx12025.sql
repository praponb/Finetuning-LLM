WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE product IN ('HEALTH', 'HOME')
),
stacked AS (
    -- Stack the branches
    SELECT id, holder_id AS key_id, 'policy' AS origin
    FROM cleaned
    WHERE holder_id IS NOT NULL
    UNION ALL
    SELECT holder_id, id, 'policy_2' AS origin
    FROM policies
    WHERE start_date >= DATE '2024-10-22'
)
SELECT origin, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY origin
ORDER BY origin
