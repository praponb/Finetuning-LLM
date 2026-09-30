from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "last_restock"), "store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_last())
w_ordered = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity_avg_6_2", "id")
w_part = Window.partitionBy("product_id").orderBy("quantity_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        F.avg("quantity").over(w).alias("quantity_avg_6"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
        F.avg("quantity").over(w_ordered).alias("quantity_avg_6_2"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.launch_date"), F.col("b.cost"), F.col("b.stock_qty"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_id",
        "quantity_cume_dist",
        "quantity_avg_6_2",
        "cost",
        "stock_qty",
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "product_id",
        "cost",
        "stock_qty",
        F.lead("stock_qty").over(w_grp).alias("next_stock_qty"),
        F.lag("quantity_avg_6_2").over(w_grp).alias("prev_quantity_avg_6_2"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.last("cost", ignorenulls=True).over(w_part).alias("last_known_cost"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.seq_no",
        F.col("b.cost").alias("product_cost"),
    )
)

result = (
    enriched
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("product_cost_tier", F.when(F.col("product_cost") >= 780, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no_tier", "product_cost_tier"], ["seq_no_tier"], []],
        "seq_no_tier",
        "product_cost_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("product_cost", 0.5).alias("product_cost_p50"),
    )
    .filter(F.col("product_cost_p50") > 0)
)
