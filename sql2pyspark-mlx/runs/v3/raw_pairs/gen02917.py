from pyspark.sql import functions as F

src = df_shipments.filter(F.month("shipped_date").isin([1, 2, 4]))

summary = (
    src
    .groupBy("status", F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    summary
    .filter(F.col("max_cost") < 90)
    .select("status", "shipped_year", "dhl_count")
    .orderBy("dhl_count", F.col("shipped_year").desc())
)
