from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-30").cast("date"))
    .select(
        "*",
        F.when(F.col("revenue") >= 3400, "High").when(F.col("revenue") >= 1900, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("units") >= 36, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
        "store_id",
    )
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("units", 0.95).alias("units_p95_2"),
        F.bit_and("units").alias("units_bit_and_2"),
    )
    .filter(F.col("units_p95") < 1000000)
)
