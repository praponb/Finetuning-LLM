select status from orders except select status from accounts where balance <= 30000 order by status
