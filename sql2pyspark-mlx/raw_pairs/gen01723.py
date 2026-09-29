from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").asc_nulls_last())
w2 = Window.partitionBy("status").orderBy(F.col("age").desc_nulls_last())

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-09-18").cast("date")) & (F.col("start_date") <= "2024-02-01"))
    .select("*", F.trim("status").alias("clean_status"))
)

kept_keys = (
    valid
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") <= "2025-06-01")
        .select("customer_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("src.*", F.col("c.age"), F.col("c.signup_date"))
)

filtered = (
    enriched
    .filter((F.col("start_date") >= F.lit("2023-06-18").cast("date")) & ~F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]))
    .select(
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "clean_status",
        "age",
        "signup_date",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

scored = (
    filtered
    .filter(F.col("start_date") < "2025-07-01")
    .select(
        "customer_id",
        "status",
        "age",
        "signup_date",
        F.percent_rank().over(w).alias("age_percent_rank"),
        F.rank().over(w2).alias("age_rank"),
    )
)

result = (
    scored
    .select(
        "status",
        "age",
        "signup_date",
        "age_percent_rank",
        F.when(F.col("status").rlike("^(EXPIRED|PAUSED)$"), "GROUP_B").when(F.col("status").rlike("^(ACTIVE|CANCELLED)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
)
