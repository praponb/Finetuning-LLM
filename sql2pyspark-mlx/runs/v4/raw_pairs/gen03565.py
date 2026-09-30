from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-09-12"))) & (F.col("sale_date") > "2023-08-01"))
    .select(
        "*",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit_2"),
    )
)

result = (
    valid
    .rollup(
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        F.when(F.col("revenue_per_unit") >= 89, "LARGE").otherwise("SMALL").alias("revenue_per_unit_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.bit_or("units").alias("units_bit_or"),
    )
)
