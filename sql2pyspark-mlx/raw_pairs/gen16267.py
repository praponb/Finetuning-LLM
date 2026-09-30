from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-09-26").cast("date")) & (F.col("units") < 15))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("units") >= 34, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
        F.bit_or("units").alias("units_bit_or"),
    )
)
