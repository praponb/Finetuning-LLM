WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM tariff_windows
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        tariff,
        tariff_ref,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY valid_to DESC, id) AS valid_to_quartile,
        DENSE_RANK() OVER (ORDER BY valid_to) AS valid_to_dense_rank
    FROM prepared
)
SELECT *
FROM (
    SELECT valid_to_dense_rank, tariff, valid_to_quartile
    FROM windowed
) src
PIVOT (
    SUM(valid_to_quartile)
    FOR tariff IN ('TOU', 'FLAT')
)
ORDER BY valid_to_dense_rank
