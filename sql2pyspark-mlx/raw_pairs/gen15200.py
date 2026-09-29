from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity_percent_rank").asc_nulls_first(), "id")

enriched = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w_ordered).alias("row_seq_2"),
        F.avg("unit_price").over(w3).alias("unit_price_avg_2_prior"),
        F.percent_rank().over(w4).alias("quantity_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("unit_price_avg_2_prior") < 291)
    .select(
        "*",
        F.greatest("row_seq", "unit_price_avg_2_prior").alias("greatest_row_seq_unit_price_avg_2_prior"),
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
    )
)

enriched_order_items = (
    cleaned
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.supplier_id") == 32)
    .select("src.*", F.col("d.category"), F.col("d.launch_date"))
)

ranked = (
    enriched_order_items
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

totals = (
    ranked
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.percentile_approx("quantity_percent_rank", 0.95).alias("quantity_percent_rank_p95"),
    )
)

result = (
    totals
    .select("order_id", "home_rows", "quantity_percent_rank_p95")
    .orderBy(F.col("order_id").desc_nulls_last())
)
