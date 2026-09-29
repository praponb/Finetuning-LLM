select cost value from shipments where status = 'RETURNED' union all select amount value from payments where method = 'card' order by value desc
