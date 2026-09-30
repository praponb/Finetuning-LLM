from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("price", "id")
w6 = Window.partitionBy(F.date_trunc("WEEK", "launch_date"), "product_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") == 6))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
    .withColumn("line_total_2", (F.col("quantity") * F.col("unit_price")))
)

joined = (
    filtered
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.cost"), F.col("b.price"), F.col("b.launch_date"))
)

metrics = (
    joined
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("prev_price", F.lag("price", 1, 0).over(w2))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w3))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel") != "store")
    .select("a.*", F.col("o.amount"), F.col("o.discount"), F.col("o.order_date"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.unit_price",
        "a.line_total",
        "a.line_total_2",
        "a.cost",
        "a.price",
        "a.launch_date",
        "a.cumulative_quantity",
        "a.amount",
        "a.order_date",
        F.col("b.brand"),
        F.col("b.stock_qty"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total_2",
        "price",
        "cumulative_quantity",
        "order_date",
        F.lag("quantity").over(w5).alias("prev_quantity"),
        F.lead("quantity").over(w5).alias("next_quantity"),
        F.lead("stock_qty").over(w5).alias("next_stock_qty"),
        F.lag("stock_qty").over(w5).alias("prev_stock_qty"),
        F.row_number().over(w5).alias("row_seq"),
        F.last("cost", ignorenulls=True).over(w6).alias("last_known_cost"),
    )
)
