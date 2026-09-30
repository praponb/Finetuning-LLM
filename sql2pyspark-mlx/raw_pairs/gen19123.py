from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.orderBy("unit_price", "id")
w3 = Window.orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id")

metrics = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        F.lead("unit_price", 2).over(w).alias("next_unit_price"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

cleaned = (
    metrics
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("event_seq") - F.col("next_unit_price")).alias("event_seq_minus_next_unit_price"),
    )
)

kept_keys = (
    cleaned
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([12, 21, 40]))
        .select("order_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

cleaned_order_items = (
    kept_rows
    .filter(F.col("next_unit_price") > 240)
    .select(
        "*",
        F.round((F.col("event_seq_minus_next_unit_price") * 1.1), 2).alias("event_seq_minus_next_unit_price_adjusted"),
    )
)

filtered = (
    cleaned_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("seq_no") - F.col("event_seq")).alias("seq_no_minus_event_seq"))
)

valid = (
    filtered
    .filter(F.col("event_seq_minus_next_unit_price_adjusted").between(42, 72))
    .select(
        "*",
        F.round((F.col("next_unit_price") * 1.15), 2).alias("next_unit_price_scaled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.count("seq_no_minus_event_seq").over(w4).alias("product_id_count_seq_no_minus_event_seq"),
    )
)

result = (
    scored
    .withColumn("next_unit_price_scaled_tier", F.when(F.col("next_unit_price_scaled") >= 81, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["next_unit_price_scaled_tier", "seq_no"], ["next_unit_price_scaled_tier"], []],
        "next_unit_price_scaled_tier",
        "seq_no",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("seq_no_minus_event_seq", 0.5).alias("seq_no_minus_event_seq_p50"),
        F.grouping("next_unit_price_scaled_tier").alias("gid"),
    )
    .filter(F.col("seq_no_minus_event_seq_p50") > 0)
)
