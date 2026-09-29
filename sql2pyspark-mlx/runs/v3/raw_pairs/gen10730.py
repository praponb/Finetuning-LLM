from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1300, 2600) & (F.col("units") > 13))
    .groupBy(
        F.when(F.col("units") >= 43, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
        "product_id",
    )
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("units_range") >= 39)
    .orderBy("latest_sale_date")
)
