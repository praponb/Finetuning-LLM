from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("country").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy(F.col("rating").asc_nulls_first())
w4 = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")
w5 = Window.partitionBy("category", "supplier_id").orderBy("rating_percent_rank", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "category").orderBy("price", "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.array_contains("tags", "clearance"))
    .select(
        "*",
        F.concat_ws("-", "category", "product_name").alias("category_product_name_key"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id").eqNullSafe(F.col("p2.id")), "left")
    .select("s.*", F.col("p2.country"), F.col("p2.supplier_name"), F.col("p2.rating"))
)

metrics = (
    with_dims
    .filter(F.col("cost").between(590, 650))
    .select(
        "id",
        "category",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        "category_product_name_key",
        "dedup_rank",
        "country",
        F.avg("rating").over(w2).alias("rating_avg_3"),
        F.percent_rank().over(w3).alias("rating_percent_rank"),
    )
)

metrics_products = (
    metrics
    .select(
        "id",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        "dedup_rank",
        "country",
        "rating_avg_3",
        F.row_number().over(w4).alias("event_seq"),
        F.avg("rating_avg_3").over(w5).alias("rating_avg_3_avg_5_prior"),
        F.sum("rating_avg_3").over(w6).alias("cumulative_rating_avg_3"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

with_latest = (
    metrics_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.country",
        "s.rating_avg_3",
        "s.event_seq",
        "s.row_seq",
        F.col("c.quantity"),
    )
)

enriched = (
    with_latest
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.supplier_name"))
)

result = (
    enriched
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("rating_avg_3_tier", F.when(F.col("rating_avg_3") >= 4, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["row_seq_tier", "category", "rating_avg_3_tier"], ["row_seq_tier", "category"], []],
        "row_seq_tier",
        "category",
        "rating_avg_3_tier",
    )
    .agg(
        F.percentile_approx("rating_avg_3", [0.25, 0.5, 0.75]).alias("rating_avg_3_quartiles"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
    .filter(F.col("avg_row_seq") > 0)
)
