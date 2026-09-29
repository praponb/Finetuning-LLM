WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE holder_id >= 12
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.status_clean,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2025-09-02'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status, YEAR(holder_joined_on)) AS rows_in_status_holder_joined_on_year,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY status, YEAR(holder_joined_on)), 2) AS holder_risk_score_vs_status_holder_joined_on_yea,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id, status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        LAST_VALUE(holder_joined_on) IGNORE NULLS OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_joined_on,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY status, YEAR(holder_joined_on)), 0), 4) AS holder_risk_score_share_of_status_holder_joined_,
        NTILE(5) OVER (ORDER BY end_date ASC, id) AS end_date_quintile,
        COUNT(*) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date ASC), 3) AS end_date_percent_rank
    FROM enriched
)
SELECT
    id,
    end_date,
    status,
    holder_joined_on,
    holder_risk_score,
    rows_in_status_holder_joined_on_year,
    holder_risk_score_vs_status_holder_joined_on_yea,
    last_known_holder_joined_on,
    holder_risk_score_share_of_status_holder_joined_,
    end_date_quintile,
    running_count,
    end_date_percent_rank
FROM with_trends
ORDER BY end_date_percent_rank, holder_risk_score_share_of_status_holder_joined_, id
LIMIT 5
