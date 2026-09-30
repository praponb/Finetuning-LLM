from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter(F.year("sale_date") >= 2023)
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .withColumn("sale_date_weekofyear", F.weekofyear("sale_date"))
)

base = (
    filtered
    .filter((F.col("sale_date") >= F.lit("2024-04-05").cast("date")) & (F.col("revenue") > 2500))
    .withColumn("revenue_filled_per_units", F.round((F.col("revenue_filled") / F.col("units")), 2))
    .withColumn("revenue_rounded", F.round("revenue", 1))
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    ranked
    .filter(F.col("sale_date") >= F.lit("2024-02-29").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_filled",
        "sale_date_weekofyear",
        "revenue_rounded",
        "rn",
        (F.col("units") * F.col("revenue_filled_per_units")).alias("units_times_revenue_filled_per_units"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("sale_date_weekofyear") >= 67, "PREMIUM").otherwise("STANDARD").alias("sale_date_weekofyear_tier"),
    )
    .agg(
        F.round(F.avg("sale_date_weekofyear"), 2).alias("avg_sale_date_weekofyear"),
        F.bit_or("units").alias("units_bit_or"),
        F.round(F.avg("sale_date_weekofyear"), 2).alias("avg_sale_date_weekofyear_2"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_or("units").alias("units_bit_or_2"),
    )
    .orderBy(F.col("sale_date_weekofyear_tier").desc())
)
