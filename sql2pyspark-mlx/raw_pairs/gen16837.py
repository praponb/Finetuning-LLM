from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("first_known_unit_price", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("last7_min_quantity_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")
w6 = Window.orderBy(F.col("running_last7_min_quantity_avg_6").desc())
w7 = Window.partitionBy("product_category").orderBy("last7_min_quantity_avg_6", "id")
w8 = Window.partitionBy("category").orderBy(F.col("running_last7_min_quantity_avg_6_p90").desc())

enriched = (
    df_order_items
    .filter(F.col("order_id").isin([29, 35]))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.avg("quantity").over(w).alias("quantity_avg_6"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
    )
)

enriched_order_items = (
    enriched
    .withColumn("last7_count_quantity_avg_6", F.count("quantity_avg_6").over(w_ordered))
    .withColumn("last7_sum_quantity_avg_6", F.sum("quantity_avg_6").over(w_ordered))
    .withColumn("last7_min_quantity_avg_6", F.min("quantity_avg_6").over(w_ordered))
)

enriched_order_items_2 = (
    enriched_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.last7_min_quantity_avg_6",
        F.col("d.brand"),
        F.col("d.product_name"),
        F.col("d.category"),
    )
)

with_dims = (
    enriched_order_items_2
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.category").alias("product_category"),
        F.col("p.brand").alias("product_brand"),
    )
)

enriched_order_items_3 = (
    with_dims
    .select(
        "id",
        "product_id",
        "last7_min_quantity_avg_6",
        "product_name",
        "category",
        "product_category",
        F.sum("last7_min_quantity_avg_6").over(w4).alias("running_last7_min_quantity_avg_6"),
        F.round((F.col("last7_min_quantity_avg_6") / F.sum("last7_min_quantity_avg_6").over(w5)), 4).alias("last7_min_quantity_avg_6_share"),
    )
)

scored = (
    enriched_order_items_3
    .select(
        "id",
        "product_id",
        "last7_min_quantity_avg_6",
        "product_name",
        "category",
        "running_last7_min_quantity_avg_6",
        "last7_min_quantity_avg_6_share",
        F.percent_rank().over(w6).alias("running_last7_min_quantity_avg_6_percent_rank"),
        F.lead("last7_min_quantity_avg_6_share", 2).over(w7).alias("next_last7_min_quantity_avg_6_share"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.category",
        "e.running_last7_min_quantity_avg_6",
        "e.last7_min_quantity_avg_6_share",
        "e.next_last7_min_quantity_avg_6_share",
        F.col("b.price"),
        F.col("b.cost"),
    )
)

daily = (
    joined
    .groupBy(
        "category",
        F.when(F.col("last7_min_quantity_avg_6_share") >= 0.79, "PREMIUM").otherwise("STANDARD").alias("last7_min_quantity_avg_6_share_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.percentile_approx("running_last7_min_quantity_avg_6", 0.9).alias("running_last7_min_quantity_avg_6_p90"),
    )
    .withColumn("running_last7_min_quantity_avg_6_p90_percent_rank", F.percent_rank().over(w8))
)

result = (
    daily
    .select(
        "category",
        "last7_min_quantity_avg_6_share_tier",
        "num_categories",
        "row_count",
        "home_count",
        "running_last7_min_quantity_avg_6_p90",
        "running_last7_min_quantity_avg_6_p90_percent_rank",
    )
    .orderBy(F.col("home_count").desc_nulls_last())
)
