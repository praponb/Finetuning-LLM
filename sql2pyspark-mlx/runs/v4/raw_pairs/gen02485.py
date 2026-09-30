from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id")
w_grp = Window.partitionBy("product_id", "store_id")
w4 = Window.orderBy("sale_date", "id")
w5 = Window.orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

enriched = (
    df_store_sales
    .select(
        "*",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.min("revenue").over(w_grp).alias("product_id_min_revenue"),
        F.lag("revenue", 1).over(w4).alias("prev_revenue"),
        F.avg("revenue").over(w5).alias("revenue_avg_6"),
    )
)

result = (
    enriched
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "next_revenue",
        "revenue_share",
        "prev_revenue",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("revenue_share").desc())
)
