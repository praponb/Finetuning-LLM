from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 4, 7]) & (F.col("units") > 21))
    .select(
        "id",
        "units",
        "product_id",
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)
