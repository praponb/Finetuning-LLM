from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("end_date"), "status").orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("customer_id", "status").orderBy("credit_limit_dense_rank")
w4 = Window.orderBy(F.col("credit_limit").desc_nulls_first())

joined = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.start_date",
        "e.end_date",
        "e.status",
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
)

base = (
    joined
    .filter(F.col("end_date").between("2023-06-01", "2024-08-01"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "email",
        "credit_limit",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

remaining_keys = (
    base
    .select("customer_id", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") == "standard")
        .select("customer_id", "status")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("customer_id").isNotNull())
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
)

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"), F.col("d.name"))
)

enriched = (
    with_dims
    .select(
        "id",
        "customer_id",
        "credit_limit_share",
        "city",
        F.percent_rank().over(w3).alias("credit_limit_dense_rank_percent_rank"),
        F.dense_rank().over(w4).alias("credit_limit_dense_rank_2"),
    )
)

joined_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.credit_limit_share",
        "s.city",
        "s.credit_limit_dense_rank_percent_rank",
        F.col("b.signup_date"),
        F.col("b.name"),
    )
)

result = (
    joined_subscriptions
    .groupBy(
        F.when(F.col("credit_limit_dense_rank_percent_rank") >= 0.73, "HIGH").otherwise("LOW").alias("credit_limit_dense_rank_percent_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("credit_limit_dense_rank_percent_rank")).otherwise(0)).alias("bangkok_credit_limit_dense_rank_percent_rank"),
    )
    .filter(F.col("bangkok_credit_limit_dense_rank_percent_rank") >= 0)
)
