from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 12)
    .select(
        "id",
        "sale_date",
        "store_id",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit_2"),
    )
)
