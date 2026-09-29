from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.3, "high").when(F.col("discount") >= 0.07, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("max_discount") > 0.37)
    .drop("max_discount")
)
