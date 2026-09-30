from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("order_id")
w5 = Window.orderBy("running_avg_unit_price_per_unit_price", "id")
w6 = Window.partitionBy("order_id").orderBy("quantity", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("prev_running_max_unit_price").desc_nulls_first(), "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("order_id").isin([3, 22, 24, 26]))
    .select(
        "*",
        F.avg("unit_price").over(w2).alias("running_avg_unit_price"),
        F.sum("unit_price").over(w2).alias("running_sum_unit_price"),
        F.max("unit_price").over(w2).alias("running_max_unit_price"),
        F.avg("quantity").over(w3).alias("quantity_avg_5_prior"),
    )
)

base = (
    metrics
    .filter(F.col("product_id").isNotNull() & (F.col("running_sum_unit_price") < 500))
    .withColumn("running_avg_unit_price_per_unit_price", F.round((F.col("running_avg_unit_price") / F.col("unit_price")), 2))
)

enriched = (
    base
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w4))
    .withColumn("prev_running_max_unit_price", F.lag("running_max_unit_price", 1).over(w5))
    .withColumn("next_quantity_avg_5_prior", F.lead("quantity_avg_5_prior", 1).over(w6))
)

ranked = (
    enriched
    .withColumn("dedup_rank_2", F.row_number().over(w7))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

result = (
    ranked
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "dedup_rank",
        "running_avg_unit_price",
        "running_max_unit_price",
        "running_avg_unit_price_per_unit_price",
    )
    .orderBy(F.col("running_avg_unit_price_per_unit_price").asc_nulls_last())
)
