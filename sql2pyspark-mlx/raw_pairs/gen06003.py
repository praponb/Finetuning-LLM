from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id")

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-28").cast("date"))
    .select("*", (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units"))
)

scored = (
    latest_match
    .withColumn("last4_max_price_minus_stock_qty", F.max("price_minus_stock_qty").over(w_running))
    .withColumn("last4_count_price_minus_stock_qty", F.count("price_minus_stock_qty").over(w_running))
    .withColumn("last4_sum_price_minus_stock_qty", F.sum("price_minus_stock_qty").over(w_running))
)

metrics = (
    scored
    .filter(F.col("brand") == "Umbrella")
    .withColumn("prev_cost", F.lag("cost").over(w3))
    .withColumn("next_last4_max_price_minus_stock_qty", F.lead("last4_max_price_minus_stock_qty").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

with_dims = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.product_name",
        "s.price",
        "s.stock_qty",
        "s.tags",
        "s.supplier_id",
        "s.sale_date",
        "s.last4_max_price_minus_stock_qty",
        "s.last4_sum_price_minus_stock_qty",
        "s.prev_cost",
        F.col("p.country"),
        F.col("p.supplier_name"),
        F.col("p.rating"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "country",
        F.col("rating").alias("RATING_VAL"),
        F.col("last4_max_price_minus_stock_qty").alias("LAST4_MAX_PRICE_MINUS_STOCK_QTY"),
    )
    .unpivot(
        ids=["id", "country"],
        values=["RATING_VAL", "LAST4_MAX_PRICE_MINUS_STOCK_QTY"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
