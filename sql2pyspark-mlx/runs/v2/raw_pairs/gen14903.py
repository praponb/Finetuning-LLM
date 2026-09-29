from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(27, 85) | (F.col("carrier") == "FedEx"))
    .groupBy(F.year("delivered_date").alias("delivered_year"), "status")
    .agg(F.min("delivered_date").alias("min_delivered_date"))
    .orderBy(F.col("delivered_year").desc())
)
