from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w_ordered = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("end_date") < "2023-07-12") & F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "end_date") / 30).alias("months_since_end_date"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.monthly_fee",
        F.col("b.name"),
        F.col("b.credit_limit"),
    )
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "id",
        "monthly_fee",
        "credit_limit",
        F.min("credit_limit").over(w_ordered).alias("running_min_credit_limit"),
        F.avg("credit_limit").over(w_ordered).alias("running_avg_credit_limit"),
        F.count("credit_limit").over(w_ordered).alias("running_count_credit_limit"),
    )
)

filtered = (
    metrics
    .filter(F.col("running_count_credit_limit") != 15)
    .select(
        "id",
        "monthly_fee",
        "credit_limit",
        "running_min_credit_limit",
        "running_avg_credit_limit",
        F.round("running_avg_credit_limit", 0).alias("running_avg_credit_limit_rounded"),
    )
)

result = (
    filtered
    .select(
        "id",
        "credit_limit",
        "running_min_credit_limit",
        "running_avg_credit_limit",
    )
    .orderBy(F.col("credit_limit").desc(), "id")
)
