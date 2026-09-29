from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_minus_line_total", "id")
w2 = Window.partitionBy("order_id").orderBy("line_total", "id")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("line_total_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([14, 34]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
)

base = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("quantity") - F.col("line_total")).alias("quantity_minus_line_total"),
        F.col("line_total").cast("int").alias("line_total_int"),
    )
)

scored = (
    base
    .withColumn("quantity_minus_line_total_bucket", F.ntile(5).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("quantity_minus_line_total_avg_3_prior", F.avg("quantity_minus_line_total").over(w3))
    .withColumn("first_known_adjusted_quantity", F.first("adjusted_quantity", ignorenulls=True).over(w4))
)

joined = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.line_total",
        "s.line_total_int",
        "s.event_seq",
        "s.quantity_minus_line_total_avg_3_prior",
        F.col("p.category"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.supplier_id").isNull())
    .select(
        "e.id",
        "e.product_id",
        "e.line_total",
        "e.line_total_int",
        "e.event_seq",
        F.col("d.brand"),
        F.col("d.stock_qty"),
    )
)

result = (
    with_dims
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "line_total",
        "event_seq",
        "brand",
        F.when(F.col("brand").rlike("^(Acme|Stark)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("brand_zone"),
        F.col("max_inventory_quantity"),
    )
    .orderBy("line_total")
)
