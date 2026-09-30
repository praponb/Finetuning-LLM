from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("delivered_date") > "2023-06-01") & (F.col("cost") >= 70) & F.col("order_id").isNotNull())
)

status_group_summary = (
    valid_shipments
    .groupBy(
        F.when(F.col("status").isin(["LOST", "RETURNED"]), "group_a").otherwise("other").alias("status_group"),
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
    )
    .agg(F.max("cost").alias("max_cost"))
)

result = status_group_summary.filter(F.col("max_cost") < 71)
