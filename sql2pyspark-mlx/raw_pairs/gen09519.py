from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

scored = (
    df_products
    .select(
        "id",
        "product_name",
        "launch_date",
        F.min("price").over(w_running).alias("last7_min_price"),
        F.sum("price").over(w_running).alias("last7_sum_price"),
        F.count("price").over(w_running).alias("last7_count_price"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") > 230), "left_anti")
)

scored_products = (
    not_flagged
    .select(
        "id",
        "product_name",
        "launch_date",
        "last7_sum_price",
        F.sum("last7_sum_price").over(w_hist).alias("running_last7_sum_price"),
        F.sum("last7_count_price").over(w_hist).alias("running_last7_count_price"),
    )
)

latest_match = (
    scored_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "product_name",
        "launch_date",
        "running_last7_sum_price",
        "running_last7_count_price",
        "quantity",
        "order_item_unit_price",
        F.when(F.col("product_name").rlike("^(Backpack XL|Yoga Mat)$"), "CORE").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("product_name_zone"),
    )
)
