from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select(
        "id",
        "units",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
        F.round((F.col("units") * 100), 2).alias("adjusted_units"),
    )
    .orderBy(F.col("id").desc())
)
