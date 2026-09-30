from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") > 4), "left_anti")
)

result = (
    not_flagged
    .select("id", "category", "price", "cost", "launch_date", "supplier_id")
    .orderBy(F.col("launch_date").desc_nulls_last())
)
