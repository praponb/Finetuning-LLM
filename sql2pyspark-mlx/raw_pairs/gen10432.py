from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "id",
        "revenue",
        "units",
        "sale_date",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
    .orderBy(F.col("revenue").desc(), "id")
    .limit(20)
)
