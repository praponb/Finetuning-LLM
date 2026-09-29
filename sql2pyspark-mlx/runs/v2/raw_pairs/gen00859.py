from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.countDistinct(F.col("p.amount")).alias("unique_orders"),
    )
    .filter(F.col("unique_orders") > 1)
)
