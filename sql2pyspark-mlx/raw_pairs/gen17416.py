from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("channel") == "partner") & (F.col("status") == "RETURNED"))
    .withColumn("amount_whole", F.floor("amount"))
)

summary = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
)

result = summary.select("channel", "min_amount", "pct_mobile")
