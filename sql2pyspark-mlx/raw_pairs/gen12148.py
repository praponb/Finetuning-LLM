from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("category").orderBy("stock_qty", "id")

filtered = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.concat("country", F.lit("/"), "supplier_name").alias("country_supplier_name"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country_supplier_name",
        F.col("c.stock_qty"),
        F.col("c.category"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("stock_qty").between(139, 172))
    .withColumn("category_prefix", F.substring("category", 1, 2))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isin([12, 38])), "left_semi")
)

enriched = (
    eligible
    .select(
        "id",
        "country_supplier_name",
        "stock_qty",
        "category_prefix",
        F.lead("stock_qty").over(w_running).alias("next_stock_qty"),
        F.lag("stock_qty").over(w_running).alias("prev_stock_qty"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

valid = (
    enriched
    .filter(F.col("prev_stock_qty") <= 180)
    .withColumn("next_stock_qty_times_stock_qty", (F.col("next_stock_qty") * F.col("stock_qty")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Electronics", "Fashion", "Grocery"])), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("next_stock_qty") >= 354, "HIGH").otherwise("LOW").alias("next_stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
)
