from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .intersect(df_shipments.select(F.col("shipped_date").alias("sale_date")))
    .orderBy(F.col("sale_date").desc())
)
