from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("customer_id") == 16)
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        F.sum("discount").over(w_hist).alias("last7_sum_discount"),
        F.count("discount").over(w_hist).alias("last7_count_discount"),
        F.min("discount").over(w_hist).alias("last7_min_discount"),
        F.max("discount").over(w_hist).alias("last7_max_discount"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
