from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2023-05-16") & (F.col("units") < 26) & (F.col("revenue") <= 3500))
    .groupBy("product_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)
