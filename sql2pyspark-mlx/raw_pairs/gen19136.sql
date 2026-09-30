WITH cleaned AS (
    SELECT *, TRIM(device) AS clean_device
    FROM sessions
    WHERE started_at >= DATE '2023-03-16'
      AND customer_id IS NULL
),
with_dims AS (
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        e.started_at,
        e.clean_device,
        p.age,
        p.segment
    FROM cleaned e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
scored AS (
    SELECT
        id,
        age,
        segment,
        LEAD(age) OVER (PARTITION BY segment ORDER BY started_at, id) AS next_age,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY started_at, id) AS event_seq,
        SUM(age) OVER (PARTITION BY browser ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age
    FROM with_dims
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (1, 9, 12)
        )
),
totals AS (
    SELECT
        CASE WHEN age >= 71 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
        COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
        ROUND(SUM(cumulative_age) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS cumulative_age_per_smb,
        ROUND(SUM(cumulative_age) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS cumulative_age_per_smb_2,
        MAX(event_seq) AS max_event_seq
    FROM not_flagged
    GROUP BY CASE WHEN age >= 71 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    age_tier,
    cumulative_age_per_smb,
    cumulative_age_per_smb_2,
    max_event_seq
FROM totals
ORDER BY age_tier DESC;
