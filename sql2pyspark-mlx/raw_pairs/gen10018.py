from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("element"))

cleaned = (
    flattened
    .filter(~F.col("element").eqNullSafe(F.lit("SPRING10")))
    .withColumn("element_upper", F.upper("element"))
    .withColumn("element_clean", F.coalesce("element", F.lit("NONE")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("element").isNotNull())
    .withColumn("clean_element", F.trim("element"))
    .withColumn("resolved_element", F.coalesce("element", F.lit("OTHER")))
)

base = (
    cleaned_carts
    .filter(F.col("element").isNotNull())
    .select(
        "id",
        "element",
        "element_upper",
        "element_clean",
        "clean_element",
        "resolved_element",
        F.length("element").alias("element_length"),
        F.coalesce("element", F.lit("DIRECT")).alias("element_filled"),
    )
)

result = (
    base
    .groupBy("element")
    .agg(
        F.count_if(F.col("element") == "WELCOME5").alias("welcome5_events"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.count_if(F.col("element") == "BUNDLE").alias("bundle_events"),
    )
)
