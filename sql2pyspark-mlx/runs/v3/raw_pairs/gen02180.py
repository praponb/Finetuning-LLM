from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
