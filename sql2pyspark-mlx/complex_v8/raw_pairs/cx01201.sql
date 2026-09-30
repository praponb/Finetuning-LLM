WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        QUARTER(start_date) AS start_date_quarter,
        QUARTER(end_date) AS end_date_quarter,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM policies
    WHERE holder_id >= 8
      AND premium <= 4678.8
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.start_date_quarter,
        s.end_date_quarter,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount,
        c.claim_date
    FROM filtered_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        start_date,
        status,
        start_date_quarter,
        end_date_quarter,
        claim_id,
        claim_approved_amount,
        claim_date,
        ROUND(start_date_quarter / NULLIF(SUM(start_date_quarter) OVER (PARTITION BY YEAR(claim_date), holder_id), 0), 4) AS start_date_quarter_share_of_claim_date_year_hold,
        DENSE_RANK() OVER (PARTITION BY claim_id ORDER BY claim_approved_amount DESC, id, claim_id) AS claim_approved_amount_dense_rank,
        NTH_VALUE(premium, 2) OVER (PARTITION BY claim_id ORDER BY claim_approved_amount DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(claim_approved_amount - AVG(claim_approved_amount) OVER (PARTITION BY YEAR(claim_date), holder_id), 2) AS claim_approved_amount_vs_claim_date_year_holder_,
        LAG(claim_approved_amount) OVER w_claim_id_by_claim_approved_a AS prev_claim_approved_amount
    FROM line_level
    WINDOW w_claim_id_by_claim_approved_a AS (PARTITION BY claim_id ORDER BY claim_approved_amount DESC, id, claim_id)
),
status_stats AS (
    -- Avg start_date_quarter per status
    SELECT status, AVG(start_date_quarter) AS avg_start_date_quarter
    FROM ranked
    GROUP BY status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_start_date_quarter,
        ROUND(s.start_date_quarter - g.avg_start_date_quarter, 2) AS start_date_quarter_minus_avg
    FROM ranked s
    JOIN status_stats g ON g.status = s.status
    WHERE s.start_date_quarter > g.avg_start_date_quarter
),
latest AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        start_date,
        status,
        start_date_quarter,
        end_date_quarter,
        claim_id,
        claim_approved_amount,
        claim_date,
        start_date_quarter_share_of_claim_date_year_hold,
        claim_approved_amount_dense_rank,
        second_premium,
        claim_approved_amount_vs_claim_date_year_holder_,
        prev_claim_approved_amount,
        avg_start_date_quarter,
        start_date_quarter_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date, id, claim_id) AS recency_rank
        FROM vs_group
    ) ranked
    WHERE recency_rank = 1
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        start_date,
        status,
        start_date_quarter,
        end_date_quarter,
        claim_id,
        start_date_quarter_share_of_claim_date_year_hold,
        claim_approved_amount_dense_rank,
        second_premium,
        prev_claim_approved_amount,
        avg_start_date_quarter,
        start_date_quarter_minus_avg,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        CASE WHEN start_date_quarter >= 3 THEN 'HIGH' WHEN start_date_quarter >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS start_date_quarter_tier,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM latest
    WHERE status = 'CANCELLED'
)
SELECT id, claim_id, metric_name, reading
FROM (
    SELECT id, claim_id, second_premium, start_date_quarter_minus_avg
    FROM annotated
) src
UNPIVOT (
    reading FOR metric_name IN (second_premium AS m_second_premium, start_date_quarter_minus_avg AS m_start_date_quarter_minus_avg)
) unp
ORDER BY id, claim_id, metric_name
