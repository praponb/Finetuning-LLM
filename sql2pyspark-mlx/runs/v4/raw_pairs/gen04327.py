from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
    .groupBy("method")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_orders") < 9)
    .select("method", "unique_orders", "unique_statuss")
    .orderBy("method")
)
