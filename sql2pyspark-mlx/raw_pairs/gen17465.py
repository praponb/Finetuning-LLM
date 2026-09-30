from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.max("units").alias("max_units"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("min_units") < 38)
    .drop("min_units")
    .orderBy("sale_month", "max_units")
    .limit(10)
)
