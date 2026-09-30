WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        level,
        payload_json,
        context,
        COALESCE(context['version'], 'none') AS version_or_default,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
    WHERE host <> 'h2'
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_tier,
    COUNT(*) FILTER (WHERE service = 'billing') AS billing_rows,
    COUNT(*) FILTER (WHERE host = 'h4') AS h4_rows,
    APPROX_PERCENTILE(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles,
    SUM(CASE WHEN host = 'h4' THEN status_code_high ELSE 0 END) AS h4_status_code_high,
    MAX(status_code_high) FILTER (WHERE service = 'checkout') AS checkout_max_status_code_high,
    GROUPING_ID() AS grouping_level
FROM unpacked
GROUP BY ROLLUP (CASE WHEN status_code_high >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY status_code_high_tier NULLS LAST
