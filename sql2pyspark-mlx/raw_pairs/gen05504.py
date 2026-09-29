from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").asc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity_filled", "id")
w4 = Window.partitionBy("category", "store_id").orderBy(F.col("last_known_quantity_filled").desc(), "id")
w5 = Window.orderBy("price", "id")
w6 = Window.orderBy("next_last_known_quantity_filled", "id")
w7 = Window.orderBy("prev_last_known_quantity_filled", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

enriched = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_filled",
        F.rank().over(w).alias("quantity_filled_rank"),
        F.last("quantity_filled", ignorenulls=True).over(w2).alias("last_known_quantity_filled"),
        F.sum("quantity_filled").over(w2).alias("cumulative_quantity_filled"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .filter(~F.col("p.brand").isin(["Initech", "Acme", "Globex"]))
    .select("src.*", F.col("p.price"), F.col("p.cost"), F.col("p.category"))
)

enriched_inventory = (
    joined
    .select(
        "id",
        "quantity_filled",
        "last_known_quantity_filled",
        "price",
        "cost",
        F.lead("last_known_quantity_filled").over(w3).alias("next_last_known_quantity_filled"),
        F.lag("last_known_quantity_filled").over(w3).alias("prev_last_known_quantity_filled"),
        F.lead("cost").over(w3).alias("next_cost"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

enriched_inventory_2 = (
    enriched_inventory
    .select(
        "id",
        "quantity_filled",
        "last_known_quantity_filled",
        "price",
        "prev_last_known_quantity_filled",
        "next_cost",
        "row_seq",
        F.ntile(4).over(w5).alias("price_bucket"),
        F.ntile(3).over(w6).alias("next_last_known_quantity_filled_bucket"),
        F.avg("quantity_filled").over(w7).alias("quantity_filled_avg_2"),
    )
)

base_inventory = (
    enriched_inventory_2
    .filter(F.col("next_last_known_quantity_filled_bucket").isNotNull())
    .withColumn("price_times_row_seq", (F.col("price") * F.col("row_seq")))
)

result = (
    base_inventory
    .groupBy(
        F.when(F.col("last_known_quantity_filled") >= 76, "HIGH").otherwise("LOW").alias("last_known_quantity_filled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .orderBy(F.col("last_known_quantity_filled_tier").asc_nulls_first())
)
