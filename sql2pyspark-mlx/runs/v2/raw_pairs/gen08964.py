from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("status") != "IN_TRANSIT")

shipped_month_summary = (
    filtered_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    shipped_month_summary
    .select("shipped_month", "cnt", "max_cost")
    .orderBy("shipped_month", F.col("max_cost").desc())
)
