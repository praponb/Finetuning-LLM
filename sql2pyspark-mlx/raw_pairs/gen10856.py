from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("product_id").orderBy("running_quantity", "id")
w_hist = Window.partitionBy("product_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy("product_id_max_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("product_id_max_quantity", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy(F.col("next_quantity").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("unit_price") <= 190)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_adjusted"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "product_id",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.max("quantity").over(w2).alias("product_id_max_quantity"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

scored_order_items = (
    ranked
    .select(
        "id",
        "product_id_max_quantity",
        "rank_in_group",
        F.lead("quantity", 1).over(w_hist).alias("next_quantity"),
        F.last("product_id_max_quantity", ignorenulls=True).over(w5).alias("last_known_product_id_max_quantity"),
        F.lead("running_quantity", 2).over(w_hist).alias("next_running_quantity"),
    )
)

result = (
    scored_order_items
    .filter(F.col("product_id_max_quantity") <= 6)
    .select(
        "*",
        F.avg("product_id_max_quantity").over(w6).alias("product_id_max_quantity_avg_3"),
        F.ntile(3).over(w7).alias("next_quantity_bucket"),
    )
)
