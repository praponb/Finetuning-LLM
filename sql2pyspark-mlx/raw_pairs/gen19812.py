from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("qty", "id", "line_no").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("customer_id")

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.qty"),
    )
)

result = (
    exploded
    .select(
        "id",
        "line_no",
        "qty",
        F.count("line_no").over(w).alias("last4_count_line_no"),
        F.sum("line_no").over(w).alias("last4_sum_line_no"),
        F.max("line_no").over(w).alias("last4_max_line_no"),
        F.count("qty").over(w2).alias("customer_id_count_qty"),
    )
)
