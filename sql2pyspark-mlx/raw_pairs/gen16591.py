from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("brand").orderBy("last_restock", "id")
w_ordered = Window.partitionBy(F.month("launch_date")).orderBy(F.col("launch_date").desc(), "id")
w3 = Window.partitionBy("brand").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("brand", "product_id").orderBy("event_seq")
w5 = Window.partitionBy("brand").orderBy(F.col("prev_quantity").desc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-27", "2023-09-10"))
    .select("*", F.month("last_restock").alias("last_restock_month"))
)

joined = (
    filtered
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") > 580)
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
)

metrics = (
    joined
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w_running))
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w_running))
    .withColumn("next_quantity", F.lead("quantity").over(w_running))
    .withColumn("prev_quantity", F.lag("quantity").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("event_seq_2", F.row_number().over(w_ordered))
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

filtered_inventory = (
    ranked
    .filter(F.col("brand").isin(["Initech", "Umbrella"]))
    .withColumn("event_seq_filled", F.coalesce("event_seq", F.lit(0)))
)

latest = (
    filtered_inventory
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select(
        "id",
        "quantity",
        "last_restock",
        "brand",
        "stock_qty",
        "prev_stock_qty",
        "next_stock_qty",
        "next_quantity",
        "prev_quantity",
        F.when(F.col("brand").rlike("^(Initech|Globex)$"), "CORE").when(F.col("brand").rlike("^(Acme|Umbrella)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy(F.col("quantity").desc(), "prev_quantity")
)
