from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.orderBy("quantity", "id")
w4 = Window.orderBy("last_restock", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("store_id_min_quantity", "id")
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy("next_store_id_min_quantity", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-08-04")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

metrics = (
    cleaned
    .withColumn("store_id_min_quantity", F.min("quantity").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

enriched = (
    metrics
    .filter(F.col("seq_no") == 5)
    .select(
        "id",
        "product_id",
        "store_id_min_quantity",
        "seq_no",
        F.lead("store_id_min_quantity", 1).over(w3).alias("next_store_id_min_quantity"),
        F.avg("quantity").over(w4).alias("quantity_avg_2_prior"),
        F.ntile(5).over(w5).alias("store_id_min_quantity_bucket"),
    )
)

enriched_inventory = (
    enriched
    .withColumn("store_id_min_quantity_bucket_share", F.round((F.col("store_id_min_quantity_bucket") / F.sum("store_id_min_quantity_bucket").over(w6)), 4))
    .withColumn("prev_next_store_id_min_quantity", F.lag("next_store_id_min_quantity", 2).over(w7))
)

result = (
    enriched_inventory
    .select(
        "id",
        "product_id",
        "store_id_min_quantity",
        "seq_no",
        "next_store_id_min_quantity",
        "store_id_min_quantity_bucket",
        "prev_next_store_id_min_quantity",
    )
)
