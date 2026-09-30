from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("num_items", "id")

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        F.col("attributes")["device"].alias("device"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .select(
        "id",
        "coupon_codes",
        "num_items",
        F.lag("num_items").over(w_hist).alias("prev_num_items"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.lead("num_items", 1).over(w_hist).alias("next_num_items"),
    )
)
