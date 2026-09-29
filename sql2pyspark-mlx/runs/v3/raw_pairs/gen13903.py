from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("cost").isNotNull() & (F.col("delivered_date") < "2025-08-11"))
)

shipped_year_summary = (
    recent_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
    )
)

result = (
    shipped_year_summary
    .filter(F.col("ups_count") >= 3)
    .select("shipped_year", "returned_count", "ups_count")
    .orderBy(F.col("ups_count").desc())
)
