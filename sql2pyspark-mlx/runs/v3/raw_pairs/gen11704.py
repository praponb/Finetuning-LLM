from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "UPS") & (F.col("delivered_date") <= "2025-04-01"))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(F.max("cost").alias("max_cost"))
    .orderBy(F.col("max_cost").desc(), F.col("shipped_quarter").desc())
)
