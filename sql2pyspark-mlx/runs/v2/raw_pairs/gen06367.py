from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("pct_ups") >= 65)
    .select("status", "pct_ups", "min_cost")
    .orderBy("pct_ups")
)
