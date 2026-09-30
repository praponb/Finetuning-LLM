from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2025-02-01") & (F.month("sale_date").isin([8, 9, 12]) | (F.col("units") < 36)))
    .select(
        "id",
        "sale_date",
        "store_id",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("store_id").desc())
)
