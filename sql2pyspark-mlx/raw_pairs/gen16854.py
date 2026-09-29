from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("store_id").isin([1, 3, 4, 6]) & ((F.col("units") >= 22) | (F.col("revenue") < 3600))))
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .orderBy("first_sale_date")
)
