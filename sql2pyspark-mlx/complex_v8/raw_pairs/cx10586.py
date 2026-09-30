from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

enriched = (
    filtered_policys
    .select(
        "*",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("product").isNotNull())
    .select("*", F.lower(F.trim(F.col("product"))).alias("product_clean"))
)

enriched_3 = (
    enriched_2
    .filter(F.col("product_or_unknown") == "TRAVEL")
    .select(
        "*",
        F.concat(F.col("product_clean"), F.lit("#"), F.col("end_date_dayofweek").cast("string")).alias("product_clean_ref"),
        F.length(F.trim(F.col("product_clean"))).alias("product_clean_len"),
        F.when(F.col("end_date_dayofweek") >= 4, "A").when(F.col("end_date_dayofweek") >= 2, "B").otherwise("C").alias("end_date_dayofweek_tier"),
    )
)

result = (
    enriched_3
    .groupingSets(
        [["product_or_unknown", "end_date_dayofweek_tier"], ["product_or_unknown"]],
        "product_or_unknown", "end_date_dayofweek_tier",
    )
    .agg(
        F.grouping("product_or_unknown").alias("is_total_product_or_unknown"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(
        F.col("product_or_unknown").asc_nulls_last(),
        F.col("end_date_dayofweek_tier").asc_nulls_last(),
    )
)
