from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("cost").between(75, 76) & (F.col("shipped_date") <= "2024-12-13"))
)

summary = (
    recent_shipments
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    summary
    .filter(F.col("unique_carriers") < 9)
    .select("status", "unique_carriers", "total_cost")
    .orderBy("total_cost", "status")
    .limit(1)
)
