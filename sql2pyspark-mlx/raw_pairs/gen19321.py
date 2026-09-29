from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("sale_date").asc_nulls_first(), "id")
w3 = Window.partitionBy(F.month("launch_date"), "product_id").orderBy("launch_date", "id")
w4 = Window.partitionBy("product_id").orderBy("launch_date", "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 9]))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

joined = (
    filtered
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.id", "e.product_id", "e.sale_date", "e.units", F.col("d.launch_date"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "launch_date")
)

metrics = (
    latest
    .select(
        "id",
        "product_id",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    metrics
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("id", "product_id", "row_seq", "seq_no", F.col("avg_order_item_quantity"))
)
