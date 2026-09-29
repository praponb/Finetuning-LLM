from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 85, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
    )
)
