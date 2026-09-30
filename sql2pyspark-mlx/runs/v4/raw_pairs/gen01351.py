from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

scored = (
    df_store_sales
    .select(
        "*",
        F.last("revenue", ignorenulls=True).over(w).alias("last_known_revenue"),
        F.rank().over(w2).alias("revenue_rank"),
    )
)

result = (
    scored
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_rank",
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("sale_date").asc_nulls_first())
)
