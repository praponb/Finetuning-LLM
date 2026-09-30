from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 4) & (F.col("revenue") <= 1600))
    .select(
        "id",
        "store_id",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
    .orderBy(F.col("revenue_per_unit").desc(), F.col("id").desc())
    .limit(5)
)
