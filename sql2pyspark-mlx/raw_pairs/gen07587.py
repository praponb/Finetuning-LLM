from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").asc_nulls_first(), "id")
w2 = Window.partitionBy("category", "brand").orderBy(F.col("launch_date").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.date_trunc("YEAR", "launch_date")).orderBy(F.col("quantity_rounded").desc(), "id")
w5 = Window.orderBy("last_restock", "id")
w6 = Window.partitionBy("product_id", "store_id").orderBy(F.col("prev_quantity_rounded").asc_nulls_first(), "id")
w7 = Window.partitionBy("store_id").orderBy("rank_in_group", "id")

cleaned = (
    df_inventory
    .filter(F.month("last_restock").isin([6, 9, 10]))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    ranked
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.quantity_rounded",
        "a.rn",
        F.col("b.launch_date"),
        F.col("b.category"),
        F.col("b.brand"),
    )
)

ranked_inventory = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_rounded",
        "launch_date",
        "rank_in_group",
        F.avg("quantity").over(w3).alias("quantity_avg_6_prior"),
        F.ntile(3).over(w4).alias("quantity_rounded_bucket"),
        F.lag("quantity_rounded", 2, 0).over(w5).alias("prev_quantity_rounded"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group_2", F.row_number().over(w7))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_id",
        "quantity_rounded",
        F.col("prev_quantity_rounded").alias("PREV_QUANTITY_ROUNDED"),
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["quantity_rounded", "PREV_QUANTITY_ROUNDED"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
