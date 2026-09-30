from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_int_filled").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low"))
)

base = (
    cleaned
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") >= 8))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_level",
        F.coalesce("unit_price_int", F.lit(0)).alias("unit_price_int_filled"),
    )
)

filtered = (
    base
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("unit_price") / F.nullif("unit_price_int_filled", F.lit(0))).alias("unit_price_per_unit_price_int_filled"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned_order_items = (
    ranked
    .filter(F.col("unit_price_int_filled") > 80)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_level",
        "unit_price_int_filled",
        "unit_price_per_unit_price_int_filled",
        (F.col("unit_price_per_unit_price_int_filled") - F.col("unit_price")).alias("unit_price_per_unit_price_int_filled_minus_unit_price"),
        (F.col("unit_price_per_unit_price_int_filled") + F.col("unit_price_int_filled")).alias("unit_price_per_unit_price_int_filled_plus_unit_price_int_filled"),
    )
)

result = (
    cleaned_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 390, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.sum("unit_price_per_unit_price_int_filled_plus_unit_price_int_filled").alias("total_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled"),
        F.percentile_approx("unit_price_per_unit_price_int_filled", [0.25, 0.5, 0.75]).alias("unit_price_per_unit_price_int_filled_quartiles"),
        F.round(F.avg("unit_price_per_unit_price_int_filled_plus_unit_price_int_filled"), 2).alias("avg_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled"),
    )
    .filter(F.col("total_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled") > 0)
    .orderBy(
        F.col("avg_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled").desc_nulls_last(),
    )
)
