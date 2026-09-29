from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.count("*").alias("num_store_sales"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("num_store_sales") >= 9)
    .select("store_id", "sale_quarter", "num_store_sales", "avg_units")
    .orderBy("sale_quarter")
)
