from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")

scored = (
    df_orders
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.last("discount", ignorenulls=True).over(w2).alias("last_known_discount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

result = (
    scored
    .groupBy(
        "store_id",
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.when(F.col("shipping_fee") >= 38, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("pending_count") < 1)
)
