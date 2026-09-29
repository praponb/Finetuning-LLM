from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w_grp = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_first())
w_grp_2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w_grp_3 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy(F.col("quantity").desc_nulls_last())
w_part = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())

result = (
    df_inventory
    .filter(F.col("quantity") != 195)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.rank().over(w_grp).alias("quantity_rank"),
        F.ntile(3).over(w_grp_2).alias("quantity_bucket"),
        F.cume_dist().over(w_grp_3).alias("quantity_cume_dist"),
        F.rank().over(w_part).alias("quantity_rank_2"),
    )
)
