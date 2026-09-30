select amount, coalesce(method, 'N/A') method_filled, cast(amount as int) amount_int from payments where status = 'SUCCESS' and paid_at < '2023-09-02' and to_date(paid_at) < '2025-01-01'
