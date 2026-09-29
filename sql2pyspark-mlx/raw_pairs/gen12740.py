from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id")
w2 = Window.partitionBy("country")
w3 = Window.orderBy(F.col("age_share").desc())
w4 = Window.partitionBy("element")

flattened = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.element",
        F.col("b.country"),
        F.col("b.signup_date"),
        F.col("b.age"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.name"),
        F.col("d.signup_date").alias("customer_signup_date"),
    )
)

with_windows = (
    with_dims
    .withColumn("customer_id_count_age", F.count("age").over(w_part))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w2)), 4))
)

scored = (
    with_windows
    .filter(F.col("name").like("F%"))
    .select(
        "*",
        F.stddev_samp("age_share").over(w_part).alias("customer_id_stddev_samp_age_share"),
        F.rank().over(w3).alias("age_share_rank"),
        F.round((F.col("customer_id_count_age") / F.sum("customer_id_count_age").over(w2)), 4).alias("customer_id_count_age_share"),
        F.min("age").over(w4).alias("element_min_age"),
    )
)

base = (
    scored
    .filter(F.col("name").like("%Lee%"))
    .select(
        "id",
        "customer_id",
        "element",
        "country",
        "signup_date",
        "age",
        "name",
        "customer_signup_date",
        "customer_id_count_age",
        "age_share",
        "customer_id_stddev_samp_age_share",
        "element_min_age",
        (F.col("age_share_rank") * F.col("customer_id_stddev_samp_age_share")).alias("age_share_rank_times_customer_id_stddev_samp_age_share"),
    )
)

with_dims_carts = (
    base
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country").alias("customer_country"), F.col("p.segment"))
)

result = (
    with_dims_carts
    .groupBy("segment")
    .agg(
        F.round(F.sum("customer_id_count_age") / F.nullif(F.count(F.when(F.col("element") == "WELCOME5", 1)), F.lit(0)), 2).alias("customer_id_count_age_per_welcome5"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.avg(F.when(F.col("country") == "DE", F.col("age_share_rank_times_customer_id_stddev_samp_age_share"))).alias("de_avg_age_share_rank_times_customer_id_stddev_samp_age_share"),
    )
)
