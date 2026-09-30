from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([5, 6]))
    .select(
        "units",
        "sale_date",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy("revenue", F.col("units").desc())
)
