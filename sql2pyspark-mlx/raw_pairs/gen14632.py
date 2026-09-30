from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("weeks_since_last_restock").desc_nulls_last())
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("weeks_since_last_restock", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-05-06").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 7).alias("weeks_since_last_restock"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    ranked
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-07-01"))) & (F.col("quantity") >= 124))
    .withColumn("weeks_since_last_restock_rounded", F.round("weeks_since_last_restock", 1))
)

metrics = (
    valid
    .select(
        "*",
        F.sum("weeks_since_last_restock_rounded").over(w2).alias("cumulative_weeks_since_last_restock_rounded"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity",
        "s.last_restock",
        "s.latest_rank",
        "s.weeks_since_last_restock_rounded",
        "s.cumulative_weeks_since_last_restock_rounded",
        "s.last_known_quantity",
        "s.seq_no",
        F.col("b.brand"),
        F.col("b.price"),
        F.col("b.category"),
    )
)

result = (
    with_dims
    .withColumn("price_tier", F.when(F.col("price") >= 1180, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .groupingSets(
        [["price_tier", "product_id", "activity_month"], ["price_tier", "product_id"]],
        "price_tier",
        "product_id",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("quantity"))).alias("umbrella_total_quantity"),
        F.max("last_known_quantity").alias("max_last_known_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("price_tier").alias("gid"),
    )
    .orderBy(F.col("price_tier").desc_nulls_last())
)
