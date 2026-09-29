from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w_part = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last())
w6 = Window.orderBy(F.col("unit_price").desc_nulls_last())
w7 = Window.partitionBy("product_id").orderBy("unit_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id")
w9 = Window.orderBy(F.col("unit_price_bucket").desc(), "id")
w10 = Window.partitionBy("product_id").orderBy(F.col("unit_price_bucket_bucket").desc_nulls_first())

enriched = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.sum("quantity").over(w_part).alias("running_quantity"),
        F.ntile(10).over(w3).alias("unit_price_bucket"),
        F.corr("unit_price", "quantity").over(w4).alias("corr_unit_price_quantity"),
        F.percent_rank().over(w5).alias("unit_price_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("running_quantity") == 17)
    .withColumn("unit_price_whole", F.floor("unit_price"))
    .withColumn("running_quantity_rounded", F.round("running_quantity", 0))
)

with_windows = (
    filtered
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w6))
    .withColumn("cumulative_quantity_percent_rank", F.sum("quantity_percent_rank").over(w7))
)

scored = (
    with_windows
    .filter(F.col("unit_price") >= 250)
    .withColumn("product_id_max_quantity_percent_rank", F.max("quantity_percent_rank").over(w8))
    .withColumn("unit_price_bucket_bucket", F.ntile(4).over(w9))
)

deduped = (
    scored
    .withColumn("dedup_rank", F.rank().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity_percent_rank",
        "s.running_quantity",
        "s.unit_price_bucket",
        "s.unit_price_percent_rank",
        "s.unit_price_whole",
        "s.running_quantity_rounded",
        "s.unit_price_dense_rank",
        "s.product_id_max_quantity_percent_rank",
        "s.dedup_rank",
        F.col("d.cost"),
    )
)

base = (
    with_dims
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity_percent_rank",
        "running_quantity",
        "unit_price_bucket",
        "unit_price_percent_rank",
        "unit_price_whole",
        "running_quantity_rounded",
        "unit_price_dense_rank",
        "product_id_max_quantity_percent_rank",
        "dedup_rank",
        "cost",
        F.round("unit_price_dense_rank", 0).alias("unit_price_dense_rank_rounded"),
    )
)

result = (
    base
    .select(
        "id",
        F.col("running_quantity_rounded").alias("RUNNING_QUANTITY_ROUNDED"),
        F.col("unit_price_dense_rank_rounded").alias("UNIT_PRICE_DENSE_RANK_ROUNDED"),
        F.col("cost").alias("COST_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["RUNNING_QUANTITY_ROUNDED", "UNIT_PRICE_DENSE_RANK_ROUNDED", "COST_VAL"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
