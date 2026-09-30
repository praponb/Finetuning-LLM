from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "DHL") & (F.col("shipped_date") >= "2024-10-28") & (F.col("cost") <= 44))
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
        F.when(F.col("cost") >= 99, "high").when(F.col("cost") >= 21, "mid").otherwise("low").alias("cost_bucket"),
        "order_id",
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("cost_count", "delivered_cohort", "cost_bucket", "order_id")
    .limit(10)
)
