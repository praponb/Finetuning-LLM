from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy("cost", "id")
w6 = Window.partitionBy("category").orderBy(F.col("row_count").desc())

enriched = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.row_number().over(w4).alias("seq_no_3"),
    )
)

enriched_inventory = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.category"), F.col("p.launch_date"), F.col("p.cost"))
)

metrics = (
    enriched_inventory
    .withColumn("prev_cumulative_quantity", F.lag("cumulative_quantity").over(w5))
    .withColumn("next_seq_no", F.lead("seq_no").over(w5))
    .withColumn("prev_seq_no", F.lag("seq_no").over(w5))
    .withColumn("event_seq", F.row_number().over(w5))
)

joined = (
    metrics
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.stock_qty") <= 282)
    .select(
        "src.id",
        "src.product_id",
        "src.last_restock",
        "src.seq_no",
        "src.seq_no_2",
        "src.seq_no_3",
        "src.next_seq_no",
        "src.prev_seq_no",
        F.col("b.cost").alias("product_cost"),
    )
)

enriched_inventory_2 = (
    joined
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.brand"), F.col("b.category"))
)

result = (
    enriched_inventory_2
    .groupBy("category", "next_seq_no", "prev_seq_no")
    .agg(
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sum("prev_seq_no").alias("total_prev_seq_no"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .withColumn("row_count_percent_rank", F.percent_rank().over(w6))
)
