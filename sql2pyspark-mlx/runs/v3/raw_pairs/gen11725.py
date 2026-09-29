from pyspark.sql import functions as F

src = df_shipments.filter(F.col("shipped_date") <= "2025-05-24")

summary = (
    src
    .groupBy("carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)

result = summary
