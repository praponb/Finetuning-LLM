from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("units") < 12)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    base
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([3, 10]))
        .select("product_id", "store_id")
    )
)
