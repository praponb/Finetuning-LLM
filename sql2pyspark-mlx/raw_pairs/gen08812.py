from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w_hist = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("first_known_amount").desc(), "id")
w5 = Window.partitionBy("status")
w_grp = Window.partitionBy("status").orderBy("first_known_amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("amount_bucket_share").desc_nulls_last())

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-08-04").cast("date"))
    .select(
        "id",
        "customer_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("shipping_fee") - F.col("amount")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "status",
        F.first("amount", ignorenulls=True).over(w_recent).alias("first_known_amount"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(4).over(w_hist).alias("amount_bucket"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "status",
        F.ntile(5).over(w4).alias("first_known_amount_bucket"),
        F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w5)), 4).alias("amount_bucket_share"),
    )
)

valid = (
    scored
    .filter(F.col("amount_bucket_share") >= 0.6)
    .withColumn("adjusted_first_known_amount_bucket", F.round((F.col("first_known_amount_bucket") * 1.15), 2))
)

result = (
    valid
    .select(
        "id",
        "first_known_amount_bucket",
        "amount_bucket_share",
        "adjusted_first_known_amount_bucket",
        F.max("adjusted_first_known_amount_bucket").over(w_grp).alias("running_max_adjusted_first_known_amount_bucket"),
        F.count("adjusted_first_known_amount_bucket").over(w_grp).alias("running_count_adjusted_first_known_amount_bucket"),
        F.sum("adjusted_first_known_amount_bucket").over(w_grp).alias("running_sum_adjusted_first_known_amount_bucket"),
        F.percent_rank().over(w7).alias("amount_bucket_share_percent_rank"),
    )
)
