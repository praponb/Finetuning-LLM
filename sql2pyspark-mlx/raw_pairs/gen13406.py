from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter((F.col("status") == "LOST") | F.col("status").isin(["IN_TRANSIT", "RETURNED", "DELIVERED"]))
)

summary = (
    filtered_shipments
    .groupBy(
        F.when(F.col("cost") >= 76, "high").when(F.col("cost") >= 39, "mid").otherwise("low").alias("cost_bucket"),
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
)

result = summary
