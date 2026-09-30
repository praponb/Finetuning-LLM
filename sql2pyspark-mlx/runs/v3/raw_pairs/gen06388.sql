select * from (select quarter(paid_at) as paid_quarter, method, amount from payments) pivot (sum(amount) for method in ('bank_transfer', 'promptpay')) order by paid_quarter
