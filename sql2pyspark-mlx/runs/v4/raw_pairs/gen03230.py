from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("store_id", "channel").orderBy("shipping_fee")
w3 = Window.partitionBy("store_id", "channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

scored = (
    df_orders
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w2).alias("shipping_fee_rank"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
    )
)

result = (
    scored
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        "row_seq",
        "shipping_fee_rank",
        "first_known_channel",
        F.when(F.col("channel").rlike("^(web|store)$"), "GROUP_A").when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_B").otherwise("CORE").alias("channel_zone"),
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("discount").asc_nulls_last(), "sum_inventory_quantity")
)
