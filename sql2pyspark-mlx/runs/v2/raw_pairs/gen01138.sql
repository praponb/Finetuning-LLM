select status from accounts where opened_date between '2024-05-10' and '2025-01-09' union select status from payments where to_date(paid_at) >= '2023-10-01'
