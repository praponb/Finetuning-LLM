from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS"]))
    .groupBy("method")
    .pivot("status", ["SUCCESS", "REFUNDED", "FAILED"])
    .agg(F.count("amount"))
)
