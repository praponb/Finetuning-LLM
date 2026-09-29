from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 8, 9]) & (F.col("sale_date") < "2023-08-01"))
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)
