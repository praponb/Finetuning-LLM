from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
        F.concat_ws("|", F.col("status"), F.col("product")).alias("status_product_label"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

tagged = (
    filtered
    .select(
        "*",
        F.year(F.col("start_date")).alias("start_date_year"),
        F.concat_ws("|", F.col("product"), F.col("status_ref")).alias("product_status_ref_label"),
    )
)

derived = tagged.withColumn("start_date_month_date", F.to_date(F.col("start_date_month")))

enriched = (
    derived
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "start_date_month",
        "status_product_label",
        "status_ref",
        "start_date_year",
        "start_date_month_date",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

result = (
    enriched
    .groupBy("start_date_month_date")
    .pivot("status", ["LAPSED", "CANCELLED", "EXPIRED", "ACTIVE"])
    .agg(F.max("premium_bucket"))
    .orderBy("start_date_month_date")
)
