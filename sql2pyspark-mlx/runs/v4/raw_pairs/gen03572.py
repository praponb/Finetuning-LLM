from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock_day").asc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w_ordered_2 = Window.partitionBy("product_id").orderBy("last_restock_day", "id")
w4 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("store_id", "product_id")
w6 = Window.orderBy("quantity_avg_6", "id")
w7 = Window.partitionBy("product_id", "store_id").orderBy("store_id_count_quantity")
w_part = Window.partitionBy("store_id").orderBy(F.col("store_id_count_last_restock_day").desc_nulls_first())
w9 = Window.partitionBy("store_id").orderBy("quantity_avg_6", "id")
w10 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w11 = Window.orderBy("store_id_count_quantity_cume_dist", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-12-07").cast("date")) & F.col("product_id").isin([3, 6, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("last_restock_day_cume_dist"),
        F.avg("quantity").over(w_ordered).alias("quantity_avg_6"),
        F.lead("quantity", 1).over(w_ordered_2).alias("next_quantity"),
        F.count("quantity").over(w4).alias("store_id_count_quantity"),
        F.count("last_restock_day").over(w_grp).alias("store_id_count_last_restock_day"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.lead("store_id_count_quantity", 1).over(w6).alias("next_store_id_count_quantity"),
        F.cume_dist().over(w7).alias("store_id_count_quantity_cume_dist"),
        F.percent_rank().over(w_part).alias("store_id_count_last_restock_day_percent_rank"),
    )
)

result = (
    enriched_inventory
    .select(
        "*",
        F.lag("last_restock_day_cume_dist", 2).over(w9).alias("prev_last_restock_day_cume_dist"),
        F.row_number().over(w10).alias("seq_no"),
        F.avg("store_id_count_quantity").over(w11).alias("store_id_count_quantity_avg_3"),
        F.ntile(3).over(w6).alias("quantity_avg_6_bucket"),
    )
)
