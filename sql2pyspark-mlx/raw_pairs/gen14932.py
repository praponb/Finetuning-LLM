from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 87)
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"), "carrier")
    .agg(
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy(F.col("fedex_count").desc())
)
