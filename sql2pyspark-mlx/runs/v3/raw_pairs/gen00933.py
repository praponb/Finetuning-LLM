from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 3, 10]) & (F.col("revenue") < 2500))
    .select(
        "id",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc(), "id")
    .limit(3)
)
