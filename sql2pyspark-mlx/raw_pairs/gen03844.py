from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("store_id").orderBy(F.col("running_sum_quantity").desc())
w4 = Window.partitionBy("store_id").orderBy("running_sum_quantity")
w5 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("product_id").orderBy("running_max_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("month", "last_restock")).orderBy(F.col("quantity_avg_6_prior").desc(), "id")

enriched = (
    df_inventory
    .withColumn("running_sum_quantity", F.sum("quantity").over(w))
    .withColumn("running_max_quantity", F.max("quantity").over(w))
    .withColumn("running_min_quantity", F.min("quantity").over(w))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
)

scored = (
    enriched
    .filter(F.col("running_sum_quantity") <= 570)
    .select(
        "id",
        "product_id",
        "last_restock",
        "running_sum_quantity",
        "running_max_quantity",
        "quantity_avg_6_prior",
        F.rank().over(w3).alias("running_sum_quantity_rank"),
        F.rank().over(w4).alias("running_sum_quantity_rank_2"),
        F.avg("quantity").over(w5).alias("quantity_avg_6_prior_2"),
    )
)

enriched_inventory = (
    scored
    .select(
        "id",
        "product_id",
        "last_restock",
        "running_max_quantity",
        "quantity_avg_6_prior",
        "running_sum_quantity_rank",
        F.sum("running_sum_quantity").over(w6).alias("running_sum_running_sum_quantity"),
        F.avg("running_sum_quantity").over(w6).alias("running_avg_running_sum_quantity"),
        F.min("running_sum_quantity").over(w6).alias("running_min_running_sum_quantity"),
        F.ntile(10).over(w7).alias("quantity_avg_6_prior_bucket"),
    )
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.running_sum_quantity_rank",
        "e.running_sum_running_sum_quantity",
        "e.running_avg_running_sum_quantity",
        "e.quantity_avg_6_prior_bucket",
        F.col("b.category"),
    )
)

result = (
    enriched_inventory_2
    .groupBy(
        "category",
        F.when(F.col("running_avg_running_sum_quantity") >= 1080, "HIGH").otherwise("LOW").alias("running_avg_running_sum_quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity_avg_6_prior_bucket", [0.25, 0.5, 0.75]).alias("quantity_avg_6_prior_bucket_quartiles"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
    )
    .orderBy(F.col("category").asc_nulls_last())
)
