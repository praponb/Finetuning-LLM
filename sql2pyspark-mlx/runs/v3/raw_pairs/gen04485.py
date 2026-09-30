from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 23)
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") >= 71)
    .orderBy(F.col("avg_units").desc())
)
