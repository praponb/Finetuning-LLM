from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        F.when(F.col("status").isin(["DELIVERED", "IN_TRANSIT"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 70)
    .orderBy("cost_range")
)
