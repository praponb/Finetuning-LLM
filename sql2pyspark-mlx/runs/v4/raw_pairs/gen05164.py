from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-02-25", "2025-08-04") & ((F.col("status") == "RETURNED") | F.col("status").isin(["DELIVERED", "LOST"])))
)

agg = (
    valid_shipments
    .groupBy(
        F.when(F.col("status").isin(["LOST", "IN_TRANSIT"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    agg
    .filter(F.col("avg_cost") > 33)
    .orderBy("status_group", "avg_cost")
)
