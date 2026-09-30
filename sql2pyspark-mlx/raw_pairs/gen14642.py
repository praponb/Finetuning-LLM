from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_ordered = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_brand").orderBy("prev_price", "id")
w5 = Window.partitionBy("product_id", "product_brand").orderBy(F.col("cumulative_last_known_quantity").desc_nulls_last(), "id")
w6 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("next_quantity", "id")
w7 = Window.partitionBy("brand").orderBy("prev_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("prev_price_bucket").desc_nulls_last())
w9 = Window.orderBy(F.col("prev_price_bucket").desc(), "id")
w10 = Window.partitionBy("product_brand", "brand")
w11 = Window.partitionBy("product_brand")
w12 = Window.partitionBy("brand")
w13 = Window.partitionBy("product_brand").orderBy("prev_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand") == "Initech")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity",
        "s.last_restock",
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("p.brand"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "brand",
        F.lag("price").over(w_grp).alias("prev_price"),
        F.lead("price").over(w_grp).alias("next_price"),
        F.lag("quantity").over(w_grp).alias("prev_quantity"),
        F.lead("quantity").over(w_grp).alias("next_quantity"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.last("quantity", ignorenulls=True).over(w_ordered).alias("last_known_quantity"),
    )
)

enriched_inventory = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.last_restock",
        "s.brand",
        "s.prev_price",
        "s.next_price",
        "s.next_quantity",
        "s.last_known_quantity",
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("p.brand").alias("product_brand"),
    )
)

enriched_inventory_2 = (
    enriched_inventory
    .select(
        "*",
        F.sum("last_known_quantity").over(w3).alias("cumulative_last_known_quantity"),
        F.ntile(3).over(w4).alias("prev_price_bucket"),
    )
)

latest = (
    enriched_inventory_2
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

enriched_inventory_3 = (
    latest
    .select(
        "id",
        "last_restock",
        "brand",
        "next_price",
        "next_quantity",
        "last_known_quantity",
        "product_name",
        "product_brand",
        "prev_price_bucket",
        "row_num",
        F.ntile(4).over(w6).alias("next_quantity_bucket"),
        F.sum("prev_price_bucket").over(w7).alias("running_prev_price_bucket"),
        F.percent_rank().over(w8).alias("prev_price_bucket_percent_rank"),
        F.ntile(10).over(w9).alias("prev_price_bucket_bucket"),
        F.max("next_quantity").over(w10).alias("product_brand_max_next_quantity"),
    )
)

scored_inventory = (
    enriched_inventory_3
    .select(
        "id",
        "last_restock",
        "next_price",
        "next_quantity",
        "last_known_quantity",
        "row_num",
        "running_prev_price_bucket",
        F.avg("product_brand_max_next_quantity").over(w11).alias("product_brand_avg_product_brand_max_next_quantity"),
        F.sum("prev_price_bucket").over(w12).alias("brand_sum_prev_price_bucket"),
        F.last("next_quantity_bucket", ignorenulls=True).over(w13).alias("last_known_next_quantity_bucket"),
        F.avg("product_brand_max_next_quantity").over(w12).alias("brand_avg_product_brand_max_next_quantity"),
    )
)

result = (
    scored_inventory
    .select(
        "id",
        "last_restock",
        "next_price",
        "last_known_quantity",
        "row_num",
        "running_prev_price_bucket",
        "product_brand_avg_product_brand_max_next_quantity",
        "brand_sum_prev_price_bucket",
        "last_known_next_quantity_bucket",
    )
    .orderBy(F.col("last_known_quantity").desc_nulls_first())
)
