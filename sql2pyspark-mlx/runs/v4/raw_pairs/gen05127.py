from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        "units",
        "store_id",
        F.round((F.col("revenue") * 1.15), 2).alias("revenue_scaled"),
    )
)
