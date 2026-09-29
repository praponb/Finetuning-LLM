from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w_ordered = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id")
w_recent_2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w6 = Window.orderBy(F.col("cost").desc(), "id")
w7 = Window.partitionBy("product_id", "brand").orderBy("unit_price", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("product_id").orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("unit_price", "id").rowsBetween(-3, -1)
w_running = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

ranked = (
    df_order_items
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "quantity", "unit_price")
)

with_windows = (
    ranked
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_ordered))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent_2))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w_recent))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w5))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.category").isin(["Electronics", "Sports"]))
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        F.col("b.price"),
        F.col("b.cost"),
        F.col("b.brand"),
    )
)

scored = (
    enriched
    .withColumn("cost_bucket", F.ntile(5).over(w6))
    .withColumn("product_id_count_quantity", F.count("quantity").over(w_recent))
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w7))
)

result = (
    scored
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w8))
    .withColumn("quantity_avg_5_prior_avg_3_prior", F.avg("quantity_avg_5_prior").over(w_hist))
    .withColumn("cost_bucket_avg_6", F.avg("cost_bucket").over(w_running))
)
