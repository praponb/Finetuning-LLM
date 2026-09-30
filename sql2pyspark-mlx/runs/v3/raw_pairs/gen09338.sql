select status from accounts where status not in ('OPEN', 'CLOSED', 'FROZEN') intersect select status from shipments where cost < 54 order by status
