from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-11-04").cast("date")) & F.col("carrier").isNotNull() & (F.col("carrier") == "FedEx"))
    .select(
        "*",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

result = (
    cleaned
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "RETURNED"])
    .agg(F.sum("cost").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
