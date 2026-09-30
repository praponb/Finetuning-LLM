from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2025)
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
    .filter(F.col("avg_cost") >= 37)
    .select("shipped_year", "avg_cost", "unique_statuss")
)
