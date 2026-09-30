from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "COMPLETED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .orderBy(F.col("discount_range").desc())
)
