WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date_2
    FROM policies
    WHERE end_date BETWEEN DATE '2025-06-08' AND DATE '2027-03-20'
),
derived AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date_3
    FROM cleaned_policys
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(premium) OVER w_status_holder_id_by_days_sin AS prev_premium,
        LAG(days_since_start_date) OVER (PARTITION BY status, holder_id ORDER BY days_since_start_date, id) AS prev_days_since_start_date,
        ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY days_since_start_date, id) AS seq_no,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id,
        ROUND(days_since_start_date_2 / NULLIF(SUM(days_since_start_date_2) OVER (PARTITION BY holder_id), 0), 4) AS days_since_start_date_2_share_of_holder_id
    FROM derived
    WINDOW w_status_holder_id_by_days_sin AS (PARTITION BY status, holder_id ORDER BY days_since_start_date, id)
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.days_since_start_date,
        s.days_since_start_date_2,
        s.days_since_start_date_3,
        s.prev_premium,
        s.prev_days_since_start_date,
        s.seq_no,
        s.premium_share_of_holder_id,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM windowed s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with CUBE
SELECT
    status,
    seq_no,
    holder_id,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(holder_risk_score), 2) AS stddev_holder_risk_score,
    ROUND(SUM(days_since_start_date_2) / NULLIF(SUM(days_since_start_date), 0), 3) AS days_since_start_date_2_to_days_since_start_date
FROM enriched_holder
GROUP BY CUBE(status, seq_no, holder_id)
ORDER BY status NULLS LAST, seq_no NULLS LAST, holder_id NULLS LAST
