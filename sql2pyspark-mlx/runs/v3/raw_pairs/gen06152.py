from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.21, 0.4))
    .groupBy("channel")
    .pivot("status", ["COMPLETED", "PENDING"])
    .agg(F.count("discount"))
    .orderBy("channel")
)
