WITH stacked AS (
    -- Stack the branches
    SELECT status AS name, premium AS amount
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-07-07'
    UNION ALL
    SELECT status, premium
    FROM policies
    WHERE status = 'LAPSED'
      AND YEAR(start_date) = 2025
)
SELECT name, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM stacked
GROUP BY name
ORDER BY name
