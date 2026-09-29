from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id")
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id")

valid = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & (F.col("order_date") >= F.to_date(F.lit("2023-05-06"))))
    .withColumn("order_date_dayofweek", F.dayofweek("order_date"))
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        "order_date_dayofweek",
        F.ntile(4).over(w).alias("discount_bucket"),
        F.lead("discount", 2).over(w2).alias("next_discount"),
    )
)
