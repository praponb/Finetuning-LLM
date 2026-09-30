from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

remaining_keys = (
    df_sessions
    .select("browser", "device")
    .intersect(
        df_sessions
        .filter(F.col("customer_id") == 5)
        .select("browser", "device")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.when(F.col("country").rlike("^(US|DE)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|SG)$"), "EXTENDED").otherwise("GROUP_A").alias("country_zone"),
        F.col("max_order_discount"),
    )
    .orderBy("country_zone")
)
