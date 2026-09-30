from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(F.countDistinct("cost").alias("unique_statuss"))
    .filter(F.col("unique_statuss") < 8)
    .orderBy(F.col("unique_statuss").desc(), F.col("order_id").desc())
)
