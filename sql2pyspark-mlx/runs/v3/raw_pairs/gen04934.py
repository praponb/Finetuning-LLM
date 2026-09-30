from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-09-27", "2024-06-01") & (F.col("cost") <= 49))
    .select(
        "status",
        "order_id",
        F.when(F.col("status").isin(["IN_TRANSIT", "RETURNED"]), "Priority").otherwise("Other").alias("status_group"),
        F.round("cost", 1).alias("cost_rounded"),
    )
    .orderBy("status_group", "order_id")
)
