from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 7, 12]) & ((F.col("revenue") >= 3400) | (F.col("units") >= 21)))
    .select(
        "id",
        "sale_date",
        "product_id",
        "units",
        F.when(F.col("revenue") >= 3900, "High").when(F.col("revenue") >= 1400, "Medium").otherwise("Low").alias("revenue_band"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)
