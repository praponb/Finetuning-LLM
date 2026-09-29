from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

valid = (
    flattened
    .filter(F.col("customer_id") == 37)
    .select("*", F.coalesce("value", F.lit("unassigned")).alias("value_filled"))
)

base = (
    valid
    .filter(F.col("value").isin(["VIP", "SPRING10"]))
    .select("*", F.substring("value", 1, 3).alias("value_prefix"))
)

with_dims = (
    base
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.email"), F.col("d.segment"), F.col("d.age"))
)

valid_carts = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .select(
        "value",
        "email",
        "segment",
        "age",
        F.concat("email", F.lit("/"), "segment").alias("email_segment"),
        F.coalesce("value", F.lit("NONE")).alias("value_filled_2"),
    )
)

cleaned = (
    valid_carts
    .filter(F.col("segment").isNotNull() & (F.col("age") != 56))
    .select("*", F.upper("segment").alias("segment_upper"))
)

result = (
    cleaned
    .groupBy(
        "segment",
        F.when(F.col("age") >= 73, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.size(F.collect_set("value")).alias("num_values"),
    )
    .orderBy(F.col("age_tier").desc())
)
