from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-07-01", "2025-01-01") & F.col("channel").isin(["partner", "web", "mobile"]) & F.col("discount").between(0.22, 0.26))
    .select("status", "order_date", "channel")
)
