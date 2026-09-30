from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.amount") >= 1730, "high").when(F.col("o.amount") >= 470, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.min(F.col("s.cost")).alias("min_cost"))
    .filter(F.col("min_cost") >= 51)
    .orderBy("min_cost", F.col("amount_bucket").desc())
)
