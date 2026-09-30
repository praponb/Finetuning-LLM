from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("max_item_price").desc())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["gift"].alias("gift"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
    )
)

result = (
    parsed
    .withColumn("running_max_item_price", F.sum("max_item_price").over(w))
    .withColumn("max_item_price_cume_dist", F.cume_dist().over(w2))
    .withColumn("max_item_price_count_30m", F.count("max_item_price").over(w3))
    .withColumn("max_item_price_count_30m_2", F.count("max_item_price").over(w3))
)
