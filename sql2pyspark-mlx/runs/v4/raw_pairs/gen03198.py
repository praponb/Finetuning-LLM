from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["LOST", "IN_TRANSIT", "DELIVERED"]) & (F.col("carrier").isNotNull() | F.col("status").isin(["LOST", "DELIVERED", "RETURNED"])))
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") >= 58)
    .orderBy("avg_cost")
)
