from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["LOST", "IN_TRANSIT"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("cost_range") < 49)
    .orderBy(F.col("cost_range").desc())
)
