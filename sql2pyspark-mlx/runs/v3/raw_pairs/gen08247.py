from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") == 28) & (F.year("sale_date") >= 2024) & (F.col("units") < 21))
    .select(
        "store_id",
        F.date_sub("sale_date", 60).alias("sale_date_minus_60d"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)
