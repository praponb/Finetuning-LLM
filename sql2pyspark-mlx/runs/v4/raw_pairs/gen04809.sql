WITH valid AS (
    SELECT
        *,
        CEIL(cost) cost_whole,
        CONCAT(carrier, '/', status) carrier_status,
        COALESCE(carrier, 'N/A') carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
),
with_dims AS (
    SELECT
        e.*,
        d.order_date,
        d.status AS order_status
    FROM valid e
    INNER JOIN orders d
        ON e.order_id = d.id
),
joined AS (
    SELECT
        s.*,
        b.shipping_fee,
        b.discount,
        b.amount
    FROM with_dims s
    INNER JOIN orders b
        ON s.order_id = b.id
),
base AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        carrier_filled,
        order_status,
        amount,
        CASE WHEN cost >= 95 THEN 'High' WHEN cost >= 18 THEN 'Medium' ELSE 'Low' END cost_band,
        COALESCE(cost, 0) cost_filled,
        COALESCE(carrier, 'DIRECT') resolved_carrier
    FROM joined
    WHERE cost_whole > 48
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY carrier, status ORDER BY shipped_date DESC NULLS LAST) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
enriched AS (
    SELECT
        *,
        SUM(amount) OVER w_part cumulative_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost_filled ASC, id ASC) row_seq
    FROM deduped
    WINDOW w_part AS (PARTITION BY order_status ORDER BY cost_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT carrier
    FROM enriched
    EXCEPT
    SELECT carrier
    FROM shipments
    WHERE carrier IN ('Kerry', 'FedEx', 'UPS')
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.carrier = k.carrier
),
grouped AS (
    SELECT
        CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
        CASE WHEN amount >= 1430 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN carrier = 'Flash' THEN 1 END), 0), 2) AS amount_per_flash,
        APPROX_PERCENTILE(cumulative_amount, ARRAY(0.25, 0.5, 0.75)) AS cumulative_amount_quartiles,
        ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN order_status = 'RETURNED' THEN 1 END), 0), 2) AS row_seq_per_returned,
        SUM(CASE WHEN order_status = 'RETURNED' THEN cumulative_amount ELSE 0 END) AS returned_cumulative_amount
    FROM reconciled
    GROUP BY GROUPING SETS ((CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN amount >= 1430 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    row_seq_tier,
    amount_tier,
    status_list,
    amount_per_flash,
    cumulative_amount_quartiles
FROM grouped
ORDER BY amount_per_flash DESC NULLS FIRST, row_seq_tier DESC;
