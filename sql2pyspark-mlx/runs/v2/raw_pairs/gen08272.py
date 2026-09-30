from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2000, 2700) & F.col("product_id").isin([3, 5, 10]))
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("units_range") > 45)
    .orderBy(F.col("product_id").desc(), "units_range")
)
