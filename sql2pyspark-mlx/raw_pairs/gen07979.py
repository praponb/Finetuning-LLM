from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_ordered = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.orderBy(F.col("row_seq").asc_nulls_first())
w4 = Window.partitionBy("product_id")

metrics = (
    df_inventory
    .filter(F.col("last_restock") > "2024-06-01")
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("prev_quantity", F.lag("quantity", 2, 0).over(w_part))
    .withColumn("prev_quantity_2", F.lag("quantity", 1).over(w_ordered))
)

joined = (
    metrics
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.row_seq",
        "s.prev_quantity",
        "s.prev_quantity_2",
        F.col("p.product_name"),
    )
)

kept_keys = (
    joined
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 6]))
        .select("id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_inventory = (
    reconciled
    .select(
        "id",
        "quantity",
        "product_name",
        F.rank().over(w3).alias("row_seq_rank"),
        F.sum("prev_quantity_2").over(w4).alias("product_id_sum_prev_quantity_2"),
    )
)

result = (
    metrics_inventory
    .groupBy(
        F.when(F.col("product_id_sum_prev_quantity_2") >= 1079, "LARGE").otherwise("SMALL").alias("product_id_sum_prev_quantity_2_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("row_seq_rank", [0.25, 0.5, 0.75]).alias("row_seq_rank_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq_rank", [0.25, 0.5, 0.75]).alias("row_seq_rank_quartiles_2"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
    )
    .orderBy("distinct_ids")
)
