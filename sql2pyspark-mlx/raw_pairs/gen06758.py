from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity_adjusted", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_adjusted"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.discount"))
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined_order_items = (
    ranked
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.product_name"), F.col("d.launch_date"))
)

result = (
    joined_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_adjusted",
        "launch_date",
    )
)
