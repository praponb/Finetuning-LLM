select * from (select status, method, amount from payments) pivot (max(amount) for method in ('bank_transfer', 'cash', 'promptpay')) order by status;
