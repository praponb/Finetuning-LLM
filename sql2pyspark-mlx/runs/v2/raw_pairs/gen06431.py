from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "channel",
        F.when(F.col("amount") >= 1320, "high").when(F.col("amount") >= 760, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
