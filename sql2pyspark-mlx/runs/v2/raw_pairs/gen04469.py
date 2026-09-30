from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(19, 23))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("max_units") < 32)
    .select("sale_month", "first_sale_date", "max_units")
)
