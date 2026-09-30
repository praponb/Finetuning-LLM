from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("status").desc())
)
