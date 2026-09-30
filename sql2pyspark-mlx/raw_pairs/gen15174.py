from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.hour("created_at").between(10, 14))
    .select(
        "id",
        "created_at",
        "ship_to",
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.col("attributes")["device"].alias("device"),
    )
)
