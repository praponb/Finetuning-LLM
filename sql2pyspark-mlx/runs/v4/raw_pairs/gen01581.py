from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded_adjusted").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("quantity_rounded_adjusted", "id").rowsBetween(-5, -1)

base = (
    df_order_items
    .filter(F.col("product_id") == 10)
    .select(
        "id",
        "product_id",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

base_order_items = (
    base
    .filter(F.col("unit_price_int").isNotNull())
    .select(
        "*",
        F.round((F.col("quantity_rounded") * 1.15), 2).alias("quantity_rounded_adjusted"),
    )
)

latest = (
    base_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .select(
        "*",
        F.sum("quantity_rounded").over(w2).alias("product_id_sum_quantity_rounded"),
        F.first("quantity_rounded", ignorenulls=True).over(w3).alias("first_known_quantity_rounded"),
        F.avg("unit_price_int").over(w4).alias("unit_price_int_avg_5_prior"),
    )
)

remaining_keys = (
    with_windows
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.launch_date"), F.col("b.brand"), F.col("b.cost"))
)

kept_keys = (
    enriched
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("quantity") == 8)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.when(F.col("unit_price_int") >= 75, "HIGH").otherwise("LOW").alias("unit_price_int_tier"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count_if(F.col("brand") == "Acme").alias("acme_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("unit_price_int")).otherwise(0)).alias("stark_unit_price_int"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
