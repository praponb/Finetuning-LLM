from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["DELIVERED", "LOST"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)
