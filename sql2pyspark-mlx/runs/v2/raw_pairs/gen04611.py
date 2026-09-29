from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 15)
    .select(
        "id",
        "store_id",
        F.round((F.col("revenue") * 1.1), 2).alias("revenue_adjusted"),
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)
