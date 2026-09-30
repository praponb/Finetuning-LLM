from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("stock_qty_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("last_known_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") >= F.lit("2023-06-10").cast("date")))
    .select(
        "*",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.stock_qty_filled",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([5, 30])), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "price",
        "launch_date",
        F.avg("price").over(w2).alias("price_avg_5"),
        F.last("cost", ignorenulls=True).over(w3).alias("last_known_cost"),
        F.last("price", ignorenulls=True).over(w4).alias("last_known_price"),
    )
)

enriched_products = (
    enriched
    .filter(F.col("launch_date").between("2023-10-01", "2024-01-01"))
    .select(
        "id",
        "launch_date",
        "price_avg_5",
        "last_known_cost",
        "last_known_price",
        F.count("price_avg_5").over(w_hist).alias("running_count_price_avg_5"),
        F.min("price_avg_5").over(w_hist).alias("running_min_price_avg_5"),
        F.avg("price_avg_5").over(w_hist).alias("running_avg_price_avg_5"),
    )
)

result = (
    enriched_products
    .select("id", "last_known_price", "running_avg_price_avg_5", "last_known_cost")
    .unpivot(
        ids=["id"],
        values=["last_known_price", "running_avg_price_avg_5", "last_known_cost"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
