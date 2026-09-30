from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "PENDING") & F.col("status").isin(["RETURNED", "COMPLETED"]) & F.col("shipping_fee").between(17, 27))
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        "channel",
    )
    .agg(
        F.countDistinct("discount").alias("unique_statuss"),
        F.max("discount").alias("max_discount"),
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
    .filter(F.col("shipped_count") > 3)
    .drop("shipped_count")
    .orderBy(F.col("order_cohort").desc(), "unique_statuss")
)
