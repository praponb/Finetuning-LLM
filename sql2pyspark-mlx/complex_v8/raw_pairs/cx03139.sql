WITH cleaned AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        CASE WHEN tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group,
        ROUND(installed_kw * 1.07, 2) AS installed_kw_adj,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on
    FROM sites
    WHERE tariff NOT IN ('TOU', 'EV')
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', commissioned_on) AS days_since_commissioned_on_2,
        DATE_TRUNC('QUARTER', commissioned_on) AS commissioned_on_quarter
    FROM cleaned
    WHERE installed_kw_adj >= 265.8
)
SELECT *
FROM (
    SELECT commissioned_on_quarter, commissioned_on, tariff_group, installed_kw
    FROM scored
) src
PIVOT (
    SUM(installed_kw) AS total, MAX(installed_kw) AS max_v
    FOR tariff_group IN ('other', 'group_a', 'group_b')
)
ORDER BY commissioned_on_quarter, commissioned_on
