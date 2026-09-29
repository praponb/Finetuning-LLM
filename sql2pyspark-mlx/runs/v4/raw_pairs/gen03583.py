from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w_hist = Window.orderBy("last_restock", "id")
w_ordered = Window.partitionBy("category")
w5 = Window.partitionBy("store_id", "category").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("category").orderBy("next_quantity", "id")

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.category"))
)

metrics = (
    enriched
    .withColumn("quantity_bucket", F.ntile(10).over(w2))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w_hist))
)

scored = (
    metrics
    .select(
        "*",
        F.avg("quantity").over(w_ordered).alias("category_avg_quantity"),
        F.last("next_quantity", ignorenulls=True).over(w5).alias("last_known_next_quantity"),
        F.lead("next_quantity", 1).over(w_ordered_2).alias("next_next_quantity"),
    )
)

grouped = (
    scored
    .withColumn("year_start", F.date_trunc("YEAR", "last_restock"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .groupingSets(
        [["store_id", "year_start", "quarter_start"], ["store_id", "year_start"], ["store_id"], []],
        "store_id",
        "year_start",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    grouped
    .select(
        "store_id",
        "year_start",
        "quarter_start",
        "quantity_p50",
        "row_count",
        "num_rows",
        "distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc())
)
