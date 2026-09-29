from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-09-15").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.array_join("tags", "|").alias("tags_text"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.launch_date")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.tags_text",
        "s.sorted_tags",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date"),
    )
)

not_flagged_products = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_anti")
)

base = (
    not_flagged_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-03-21")))
    .withColumn("product_name_filled", F.coalesce("product_name", F.lit("Unknown")))
)

result = (
    base
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
