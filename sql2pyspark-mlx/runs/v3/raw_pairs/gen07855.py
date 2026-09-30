from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 9)
    .select(
        "sale_date",
        "revenue",
        "product_id",
        F.round((F.col("units") * 100), 2).alias("adjusted_units"),
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)
