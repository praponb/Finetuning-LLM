from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("cost") < 65) & (F.col("delivered_date") > "2023-12-23")) | (F.col("shipped_date") <= "2024-06-01"))
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .filter(F.col("flash_count") > 76)
    .orderBy("latest_shipped_date", "shipped_cohort", "status")
    .limit(20)
)
