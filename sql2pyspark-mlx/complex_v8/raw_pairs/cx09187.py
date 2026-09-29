from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter((F.col("premium") >= 1042.8) | (~F.col("status").isin("ACTIVE", "CANCELLED")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("CANCELLED", "LAPSED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
        F.date_add(F.col("end_date"), 90).alias("end_date_plus_90d"),
    )
)

combined_details_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_group",
        "s.end_date_plus_90d",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

per_group = (
    combined_details_holder
    .groupBy(F.year("holder_joined_on").alias("holder_joined_on_year"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("status").alias("distinct_status"),
        F.array_sort(F.collect_set("holder_region")).alias("holder_region_list"),
        F.sum(F.when(F.col("status").isNotNull(), 1).otherwise(0)).alias("n_status_is_not_null"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("row_count") >= 2)
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.avg("holder_joined_on_year").alias("overall_avg_holder_joined_on_year")))
    .filter(F.col("holder_joined_on_year") > F.col("overall_avg_holder_joined_on_year"))
    .drop("overall_avg_holder_joined_on_year")
)

result = (
    filtered
    .filter(F.col("row_count") > 4)
    .select(
        "holder_joined_on_year",
        "row_count",
        "distinct_status",
        "holder_region_list",
        "n_status_is_not_null",
    )
    .orderBy(F.col("row_count").desc_nulls_first(), "distinct_status")
)
