from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3700) & (F.col("sale_date") >= "2024-04-09"))
    .select(
        "id",
        "units",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("id").desc(), F.col("revenue_per_unit").desc())
)
