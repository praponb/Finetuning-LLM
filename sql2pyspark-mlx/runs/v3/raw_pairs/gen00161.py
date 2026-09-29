from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("sale_date").between("2023-07-10", "2023-10-19") | (F.col("units") >= 39)))
    .groupBy(
        F.when(F.col("revenue") >= 4400, "high").when(F.col("revenue") >= 1600, "mid").otherwise("low").alias("revenue_bucket"),
        "store_id",
    )
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.count("*").alias("num_store_sales"),
    )
    .filter(F.col("num_store_sales") >= 2)
    .orderBy(F.col("store_id").desc())
)
