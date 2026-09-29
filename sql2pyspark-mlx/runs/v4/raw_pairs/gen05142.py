from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date_period", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("supplier_id").isin([21, 23]))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.substring("category", 1, 3).alias("category_prefix"),
        F.date_trunc("month", "launch_date").alias("launch_date_period"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("launch_date_period") - F.lag("launch_date_period").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_last = (
    islands
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

matched_keys = (
    with_last
    .select("id", "brand")
    .subtract(
        df_products
        .filter(F.col("brand") == "Umbrella")
        .select("id", "brand")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") == 3), "left_anti")
)

result = (
    not_flagged
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        "launch_date_period",
        "is_new_burst",
        "burst_id",
        F.col("max_product_cost"),
    )
    .orderBy("supplier_id", "launch_date_period")
)
