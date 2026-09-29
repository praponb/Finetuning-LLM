from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_first())
w2 = Window.partitionBy("product_id", "order_id").orderBy("line_total")
w3 = Window.orderBy(F.col("line_total").desc())
w4 = Window.partitionBy("order_id")

base = (
    df_order_items
    .filter(F.col("quantity") > 7)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

base_order_items = (
    base
    .filter(F.col("line_total") < 32)
    .select(
        "*",
        (F.col("line_total") - F.col("unit_price")).alias("line_total_minus_unit_price"),
    )
)

kept_keys = (
    base_order_items
    .select("order_id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("order_id", "product_id")
    )
)

kept_rows = (
    base_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("line_total_minus_unit_price").between(47, 58))
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w))
    .withColumn("line_total_cume_dist", F.cume_dist().over(w2))
    .withColumn("line_total_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("order_id_sum_line_total", F.sum("line_total").over(w4))
)

result = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.unit_price_percent_rank")).alias("total_unit_price_percent_rank"),
        F.avg(F.col("a.line_total_cume_dist_2")).alias("avg_line_total_cume_dist_2"),
    )
)
