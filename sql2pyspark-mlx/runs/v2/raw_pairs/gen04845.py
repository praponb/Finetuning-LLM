from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(11, 15) & (F.col("revenue") > 1100))
    .select(
        "sale_date",
        "units",
        "product_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy("product_id")
)
