from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([7, 10, 11]))
    .groupBy("carrier")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("carrier").desc())
)
