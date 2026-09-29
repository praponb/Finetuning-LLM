from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("created_at", "id")
w_part = Window.orderBy(F.col("created_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")

parsed = (
    df_carts
    .filter(F.to_date("created_at") < "2023-08-11")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    parsed
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("event_seq_2", F.row_number().over(w_hist))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("event_seq_3", F.row_number().over(w_hist))
)
