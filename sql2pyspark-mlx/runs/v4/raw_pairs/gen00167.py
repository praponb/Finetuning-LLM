from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

remaining_keys = (
    df_customers
    .select("id", "country")
    .intersect(
        df_customers
        .filter(F.col("age") < 57)
        .select("id", "country")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.stddev_samp("age").over(w).alias("country_stddev_samp_age"),
    )
)

matched_keys = (
    scored
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("country") == "UK")
        .select("country")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "country_stddev_samp_age",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "GROUP_A").otherwise("GROUP_B").alias("segment_zone"),
    )
)
