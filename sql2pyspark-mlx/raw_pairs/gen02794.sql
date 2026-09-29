WITH with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
ranked AS (
    SELECT id, is_flagged
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS latest_rank
        FROM islands
    ) t
    WHERE latest_rank = 1
),
cleaned AS (
    SELECT *, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM ranked
    WHERE is_flagged = FALSE
),
filtered AS (
    SELECT *, IF(is_flagged, 'Yes', 'No') AS is_flagged_label_2
    FROM cleaned
    WHERE NOT is_flagged
),
cleaned_transactions AS (
    SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag
    FROM filtered
    WHERE is_flagged
),
valid AS (
    SELECT
        *,
        CASE WHEN is_flagged_flag >= 80 THEN 'High' WHEN is_flagged_flag >= 32 THEN 'Medium' ELSE 'Low' END AS is_flagged_flag_band,
        CAST(is_flagged AS INT) AS is_flagged_flag_2
    FROM cleaned_transactions
    WHERE is_flagged_flag != 20
),
filtered_transactions AS (
    SELECT
        id,
        is_flagged_label,
        is_flagged_label_2,
        is_flagged_flag,
        is_flagged_flag_band,
        is_flagged_flag_2,
        CAST(is_flagged AS INT) AS is_flagged_flag_3
    FROM valid
    WHERE is_flagged_flag_2 <= 79
)
SELECT
    id,
    is_flagged_label,
    is_flagged_label_2,
    is_flagged_flag,
    is_flagged_flag_2,
    is_flagged_flag_3
FROM filtered_transactions
ORDER BY id DESC;
