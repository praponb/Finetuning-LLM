from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("avg_units") < 58)
    .select("product_id", "avg_units", "min_sale_date")
    .orderBy("min_sale_date", "product_id")
    .limit(10)
)
