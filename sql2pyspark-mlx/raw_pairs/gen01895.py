from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_first())

latest = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest_subscriptions = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    latest_subscriptions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.signup_date"))
)

ranked = (
    joined
    .withColumn("dedup_rank_2", F.rank().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
    .select("id", "customer_id", "signup_date")
)

kept_keys = (
    ranked
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") > 120)
        .select("customer_id", "id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined_subscriptions = (
    kept_rows
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        F.col("p.country"),
        F.col("p.credit_limit"),
        F.col("p.city"),
    )
)

totals = (
    joined_subscriptions
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 17700, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["country", "credit_limit_tier"], ["country"]],
        "country",
        "credit_limit_tier",
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit"))).alias("new_york_total_credit_limit"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select(
        "credit_limit_tier",
        "new_york_total_credit_limit",
        "row_count",
        "city_list",
    )
    .orderBy(F.col("credit_limit_tier").desc())
)
