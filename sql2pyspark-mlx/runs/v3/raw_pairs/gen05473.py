from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        F.ceil("units").alias("units_whole"),
        F.round("revenue", 2).alias("revenue_rounded"),
    )
    .orderBy("id")
    .limit(10)
)
