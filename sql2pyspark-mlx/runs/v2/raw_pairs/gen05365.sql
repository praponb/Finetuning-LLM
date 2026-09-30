select status from accounts where account_type in ('SAVINGS', 'CHECKING', 'LOAN') union all select status from shipments order by status
