from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy("method")
    .pivot("status", ["SUCCESS", "FAILED"])
    .agg(F.count("amount"))
)
