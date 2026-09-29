from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1300, "high").when(F.col("amount") >= 650, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .orderBy(F.col("status").desc())
)
