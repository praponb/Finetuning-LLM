from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "last_restock")).orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy("last_known_quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("quantity_whole", "id")
w6 = Window.partitionBy(F.month("last_restock"), "product_id")

base = (
    df_inventory
    .filter(F.col("quantity") != 157)
    .withColumn("last_restock_period", F.date_trunc("year", "last_restock"))
    .withColumn("quantity_whole", F.floor("quantity"))
)

with_dims = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        "e.quantity_whole",
        F.col("p.price"),
        F.col("p.cost"),
    )
)

enriched = (
    with_dims
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .filter(F.col("b.supplier_id").isin([8, 28]))
    .select(
        "s.id",
        "s.product_id",
        "s.last_restock",
        "s.quantity_whole",
        "s.price",
        F.col("b.product_name"),
    )
)

scored = (
    enriched
    .withColumn("last_known_quantity_whole", F.last("quantity_whole", ignorenulls=True).over(w))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w2))
)

metrics = (
    scored
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.sum("last_known_quantity_whole").over(w4).alias("running_last_known_quantity_whole"),
        F.row_number().over(w5).alias("row_seq"),
        F.count("first_known_price").over(w6).alias("last_restock_month_count_first_known_price"),
    )
)

result = (
    metrics
    .withColumn("last_known_quantity_whole_tier", F.when(F.col("last_known_quantity_whole") >= 61, "HIGH").otherwise("LOW"))
    .cube("last_known_quantity_whole_tier")
    .agg(
        F.percentile_approx("price", 0.95).alias("price_p95"),
        F.avg("quantity_whole").alias("avg_quantity_whole"),
        F.grouping("last_known_quantity_whole_tier").alias("gid"),
    )
    .orderBy("avg_quantity_whole")
)
