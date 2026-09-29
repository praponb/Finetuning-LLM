from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"), "carrier")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 5)
    .orderBy("carrier", "shipped_month")
    .limit(50)
)
