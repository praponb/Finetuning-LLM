from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3000)
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.round("revenue", 0).alias("revenue_rounded"),
    )
    .orderBy(F.col("store_id").desc())
)
