from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("supplier_id").orderBy("margin", "id")
w4 = Window.partitionBy("supplier_id", "city").orderBy(F.col("prev_rating").asc_nulls_first())
w5 = Window.orderBy("prev_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

filtered = (
    df_products
    .filter((F.col("brand").eqNullSafe(F.lit("Stark"))) & F.array_contains("tags", "sale"))
    .select(
        "*",
        F.round((F.col("cost") / F.col("price")), 2).alias("margin"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

joined = (
    latest_match
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .filter(~F.col("b.city").isin(["London", "Chiang Mai"]))
    .select(
        "src.id",
        "src.cost",
        "src.stock_qty",
        "src.launch_date",
        "src.supplier_id",
        "src.margin",
        "src.resolved_brand",
        "src.revenue",
        F.col("b.rating"),
        F.col("b.city"),
    )
)

eligible = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id") == 26), "left_anti")
)

enriched = (
    eligible
    .withColumn("prev_rating", F.lag("rating").over(w2))
    .withColumn("next_margin", F.lead("margin").over(w2))
    .withColumn("prev_margin", F.lag("margin").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

enriched_products = (
    enriched
    .select(
        "id",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "revenue",
        "next_margin",
        "prev_margin",
        F.row_number().over(w3).alias("event_seq"),
        F.percent_rank().over(w4).alias("prev_rating_percent_rank"),
        F.last("seq_no", ignorenulls=True).over(w5).alias("last_known_seq_no"),
    )
)

result = (
    enriched_products
    .select(
        "id",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "next_margin",
        "prev_margin",
        "prev_rating_percent_rank",
        "last_known_seq_no",
        F.sum("last_known_seq_no").over(w6).alias("cumulative_last_known_seq_no"),
        F.ntile(10).over(w7).alias("stock_qty_bucket"),
    )
)
