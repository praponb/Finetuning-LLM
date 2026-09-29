from pyspark.sql import functions as F

valid = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("opened_on").isNotNull()))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

derived = (
    valid
    .select(
        "*",
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
        F.when(F.col("capacity") >= 11506, "large").when(F.col("capacity") >= 11005, "medium").otherwise("small").alias("capacity_tier"),
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.avg("capacity").alias("overall_avg_capacity")))
    .filter(F.col("capacity") > F.col("overall_avg_capacity"))
    .drop("overall_avg_capacity")
)

result = (
    screened
    .groupBy("capacity_tier")
    .pivot("city_or_unknown", ["Bangkok", "UNKNOWN", "Phuket"])
    .agg(F.min("capacity"))
    .orderBy("capacity_tier")
)
