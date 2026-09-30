from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 8)
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
        F.when(F.col("units") >= 43, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("unique_stores") >= 3)
)
