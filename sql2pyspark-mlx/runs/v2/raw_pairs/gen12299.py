from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 6) & F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "id",
        "units",
        "store_id",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)
