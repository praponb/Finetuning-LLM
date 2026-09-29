from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("channel").isin(["web", "store"]) & (F.col("order_date") < "2025-02-01"))
)

order_year_summary = (
    valid_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = order_year_summary.select("order_year", "row_count")
