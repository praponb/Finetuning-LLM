from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 21)
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("units").alias("total_units"),
    )
    .orderBy(F.col("total_units").desc(), "row_count")
)
