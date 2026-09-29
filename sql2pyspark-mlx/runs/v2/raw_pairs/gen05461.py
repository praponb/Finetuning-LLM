from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2024-10-06") & ((F.col("revenue") > 2700) | F.col("units").between(39, 40)))
    .select(
        "sale_date",
        "revenue",
        "store_id",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)
