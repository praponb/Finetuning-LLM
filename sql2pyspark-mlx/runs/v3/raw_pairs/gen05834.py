from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 9) & (F.col("delivered_date") <= "2025-05-29") & F.col("order_id").isin([21, 32, 40]))
    .groupBy(
        "carrier",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.count("cost").alias("cost_count"))
    .orderBy("carrier")
)
