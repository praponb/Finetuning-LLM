from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

derived = (
    cleaned
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "product_clean",
        "start_date_month_start",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
        F.when(F.col("premium") >= 3544.1, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

with_parent_holder = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.product_clean",
        "s.start_date_month_start",
        "s.premium_bucket",
        "s.premium_tier",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

screened = (
    with_parent_holder
    .crossJoin(with_parent_holder.agg(F.min("premium_bucket").alias("overall_min_premium_bucket")))
    .filter(F.col("premium_bucket") > F.col("overall_min_premium_bucket"))
    .drop("overall_min_premium_bucket")
)

result = (
    screened
    .select(
        "id",
        "premium",
        "product_clean",
        "start_date_month_start",
        "premium_bucket",
        "premium_tier",
        "holder_region",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
    )
    .orderBy(F.col("premium").desc(), F.col("id").desc_nulls_first())
    .limit(10)
)
