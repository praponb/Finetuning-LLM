WITH stacked AS (
    -- Stack the branches
    SELECT id, start_date AS event_date, holder_id
    FROM policies
    WHERE product <> 'AUTO'
    UNION ALL
    SELECT adjuster_id AS id, claim_date AS event_date, id AS holder_id
    FROM claims
    WHERE approved_amount > 9389.3
      AND status = 'IN_REVIEW'
)
SELECT id, COUNT(*) AS n_rows, SUM(holder_id) AS total_holder_id, MAX(event_date) AS latest_event_date
FROM stacked
GROUP BY id
ORDER BY id
