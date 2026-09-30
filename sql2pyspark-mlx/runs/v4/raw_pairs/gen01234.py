from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.count("*").alias("num_shipments"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") > 4)
    .orderBy(F.col("carrier").desc(), "num_shipments")
)
