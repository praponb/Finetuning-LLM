from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id")
w3 = Window.partitionBy(F.to_date("created_at"), "customer_id").orderBy(F.col("created_at").desc(), "id")

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
    )
)

result = (
    unpacked
    .filter(F.col("customer_id").isin([13, 18, 32, 33]))
    .select(
        "*",
        F.first("gift_or_default", ignorenulls=True).over(w).alias("first_known_gift_or_default"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.first("gift_or_default", ignorenulls=True).over(w).alias("first_known_gift_or_default_2"),
    )
)
