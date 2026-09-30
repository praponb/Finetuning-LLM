from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]) & F.col("order_id").isNotNull())
)

status_group_summary = (
    src
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "DELIVERED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)

result = (
    status_group_summary
    .orderBy("latest_delivered_date", F.col("first_shipped_date").desc())
)
