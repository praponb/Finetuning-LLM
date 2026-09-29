from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([1, 15, 16]))
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)

valid = (
    filtered
    .filter(F.col("unit_price_rounded").between(67, 73))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price_rounded",
        F.col("unit_price_rounded").cast("int").alias("unit_price_rounded_int"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "quantity")
)

cleaned = (
    latest
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.1), 2))
)

daily = (
    cleaned
    .groupBy("product_id")
    .agg(
        F.max("quantity_adjusted").alias("max_quantity_adjusted"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity_adjusted", 0.5).alias("quantity_adjusted_p50"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.count("*").alias("event_count"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
)

result = (
    daily
    .select(
        "product_id",
        "max_quantity_adjusted",
        "quantity_adjusted_p50",
        "quantity_p90",
        "event_count",
        "quantity_bit_and",
    )
    .orderBy(F.col("quantity_p90").asc_nulls_first())
)
