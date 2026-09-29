from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock")).orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy(F.year("launch_date")).orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy(F.date_trunc("MONTH", "launch_date"), "product_id").orderBy("launch_date", "id")
w5 = Window.partitionBy("brand").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("event_seq", "id")
w7 = Window.partitionBy("product_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.month("launch_date")).orderBy("product_id_count_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("category", "product_id").orderBy(F.col("event_seq").desc_nulls_first(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-06").cast("date"))
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty").between(226, 385))
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.quantity_whole",
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
)

enriched = (
    joined
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.count("quantity").over(w3).alias("product_id_count_quantity"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w5).alias("first_known_seq_no"),
        F.row_number().over(w6).alias("row_seq"),
        F.avg("event_seq").over(w3).alias("product_id_avg_event_seq"),
        F.sum("running_quantity").over(w7).alias("cumulative_running_quantity"),
        F.sum("event_seq").over(w8).alias("cumulative_event_seq"),
    )
)

joined_inventory = (
    enriched_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.last_restock",
        "e.quantity_whole",
        "e.launch_date",
        "e.product_id_count_quantity",
        "e.event_seq",
        "e.row_seq",
        "e.product_id_avg_event_seq",
        "e.cumulative_running_quantity",
        "e.cumulative_event_seq",
        F.col("p.category"),
        F.col("p.price"),
    )
)

latest = (
    joined_inventory
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "last_restock",
        "quantity_whole",
        "launch_date",
        "product_id_count_quantity",
        "event_seq",
        "row_seq",
        "product_id_avg_event_seq",
        "cumulative_event_seq",
    )
    .orderBy(F.col("quantity_whole").desc_nulls_last(), "launch_date")
)
