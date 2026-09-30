WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        QUARTER(closed_at) AS closed_at_quarter,
        opened_at + INTERVAL 1 DAYS AS opened_at_plus_1d,
        YEAR(closed_at) AS closed_at_year
    FROM tickets
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY requester_id, id) AS requester_id_decile,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        ROUND(AVG(requester_id) OVER w_closed_at_year_closed_at_qua, 2) AS closed_at_year_closed_at_quarter_avg_requester_i
    FROM scoped
    WINDOW w_closed_at_year_closed_at_qua AS (PARTITION BY closed_at_year, closed_at_quarter)
),
scored AS (
    -- Derive helper columns
    SELECT *, SIZE(tags) AS tags_count
    FROM with_windows
    WHERE csat > 2
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        closed_at,
        csat,
        tags,
        closed_at_quarter,
        opened_at_plus_1d,
        closed_at_year,
        last_known_closed_at,
        closed_at_year_closed_at_quarter_avg_requester_i,
        tags_count,
        DATEDIFF(DATE '2026-01-01', last_known_closed_at) AS days_since_last_known_closed_at,
        GREATEST(csat, tags_count) AS greatest_csat_tags_count
    FROM scored
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(closed_at_year - AVG(closed_at_year) OVER (PARTITION BY csat), 2) AS closed_at_year_vs_csat_avg,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat
    FROM scored_2
)
-- Final projection
SELECT
    id,
    closed_at,
    csat,
    tags,
    closed_at_quarter,
    opened_at_plus_1d,
    closed_at_year,
    closed_at_year_closed_at_quarter_avg_requester_i,
    tags_count,
    csat_share_of_csat,
    DATEDIFF(DATE '2026-01-01', opened_at_plus_1d) AS days_since_opened_at_plus_1d,
    ROUND((UNIX_TIMESTAMP(last_known_closed_at) - UNIX_TIMESTAMP(opened_at_plus_1d)) / 60, 1) AS minutes_opened_at_plus_1d_to_last_known_closed_a
FROM ranked
ORDER BY closed_at_year DESC
