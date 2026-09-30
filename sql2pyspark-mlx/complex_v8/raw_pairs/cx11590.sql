WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label,
        QUARTER(opened_at) AS opened_at_quarter
    FROM in_window
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, prior_csat_12h, opened_at_quarter, prior_rows_12h
    FROM annotated
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (prior_csat_12h, opened_at_quarter, prior_rows_12h)
) unp
