from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy("max_cost", "delivered_cohort", "carrier")
    .limit(1)
)
