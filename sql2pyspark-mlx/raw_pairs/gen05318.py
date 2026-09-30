from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 6]))
    .select(
        "id",
        "sale_date",
        "product_id",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)
