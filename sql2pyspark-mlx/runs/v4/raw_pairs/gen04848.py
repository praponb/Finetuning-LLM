from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "units",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
        F.round("units", 0).alias("units_rounded"),
    )
    .orderBy(F.col("id").desc(), "sale_date")
)
