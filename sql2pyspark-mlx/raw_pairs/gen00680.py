from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("last7_min_monthly_fee").desc_nulls_first())
w5 = Window.partitionBy("city").orderBy("age")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

scored = (
    df_subscriptions
    .withColumn("last7_count_monthly_fee", F.count("monthly_fee").over(w_running))
    .withColumn("last7_max_monthly_fee", F.max("monthly_fee").over(w_running))
    .withColumn("last7_min_monthly_fee", F.min("monthly_fee").over(w_running))
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w2))
)

valid = (
    scored
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-07-28")))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "last7_count_monthly_fee",
        "last7_max_monthly_fee",
        "last7_min_monthly_fee",
        "monthly_fee_bucket",
        F.col("last7_count_monthly_fee").cast("string").alias("last7_count_monthly_fee_num"),
    )
)

kept_keys = (
    valid
    .select("customer_id", "plan")
    .intersect(
        df_subscriptions
        .filter(F.month("end_date").isin([3, 5, 11]))
        .select("customer_id", "plan")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.customer_id",
        "src.plan",
        "src.status",
        "src.last7_count_monthly_fee",
        "src.last7_min_monthly_fee",
        "src.monthly_fee_bucket",
        "src.last7_count_monthly_fee_num",
        F.col("d.credit_limit"),
        F.col("d.age"),
        F.col("d.city"),
    )
)

scored_subscriptions = (
    with_dims
    .select(
        "customer_id",
        "status",
        "last7_count_monthly_fee",
        "monthly_fee_bucket",
        "last7_count_monthly_fee_num",
        "age",
        F.max("age").over(w3).alias("customer_id_max_age"),
        F.min("monthly_fee_bucket").over(w3).alias("customer_id_min_monthly_fee_bucket"),
        F.percent_rank().over(w4).alias("last7_min_monthly_fee_percent_rank"),
        F.dense_rank().over(w5).alias("age_dense_rank"),
    )
)

result = (
    scored_subscriptions
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "status",
        "last7_count_monthly_fee",
        "monthly_fee_bucket",
        "age",
        "customer_id_min_monthly_fee_bucket",
        "last7_min_monthly_fee_percent_rank",
        "age_dense_rank",
        F.col("sum_account_balance"),
    )
)
