from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .unionByName(df_stores.select(F.col("opened_date").alias("shipped_date")))
    .distinct()
    .orderBy(F.col("shipped_date").desc())
)
