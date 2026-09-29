select status as label from shipments where status = 'LOST' union all select country as label from sessions order by label;
