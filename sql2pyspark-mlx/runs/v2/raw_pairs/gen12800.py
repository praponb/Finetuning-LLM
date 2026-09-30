from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.35, 0.37) & F.col("order_date").between("2025-02-01", "2025-08-26"))
)
