from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("store_id").orderBy("discount")

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 23)
    .select(
        "id",
        "status",
        "discount",
        "order_date",
        "store_id",
        F.last("order_date").over(w).alias("last_order_date"),
        F.percent_rank().over(w2).alias("discount_pct_rank"),
    )
)
