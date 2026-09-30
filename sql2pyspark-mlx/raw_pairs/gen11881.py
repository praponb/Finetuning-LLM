from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        F.round((F.col("quantity") * 1.1), 2).alias("adjusted_quantity"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.status").alias("status"), F.col("dim.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("num_rows").desc())
)
