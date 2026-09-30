from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("product_id")
w_grp = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.orderBy("quantity", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("product_id", "order_id")

enriched = (
    df_order_items
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("quantity_dense_rank"),
        F.sum("unit_price").over(w2).alias("product_id_sum_unit_price"),
        F.rank().over(w_grp).alias("quantity_rank"),
        F.avg("unit_price").over(w4).alias("unit_price_avg_3_prior"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w5)), 4).alias("unit_price_share"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_dense_rank",
        "quantity_rank",
        "unit_price_avg_3_prior",
        "unit_price_share",
    )
    .orderBy(F.col("unit_price_share").desc(), "product_id")
)
