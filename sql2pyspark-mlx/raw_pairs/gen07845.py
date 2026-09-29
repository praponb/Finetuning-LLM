from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last(), "id")
w2 = Window.partitionBy("status", "segment").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("age").desc_nulls_last())
w_grp = Window.partitionBy("customer_id", "status").orderBy(F.col("age_rank").asc_nulls_last())
w5 = Window.partitionBy("status", "customer_id").orderBy("seq_no", "id")
w6 = Window.partitionBy("status").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("age_rank_dense_rank", "id")

latest = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    latest
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.city").isin(["Bangkok", "Tokyo"]))
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        F.col("c.age"),
        F.col("c.name"),
        F.col("c.segment"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "customer_id",
        "status",
        "name",
        F.row_number().over(w2).alias("seq_no"),
        F.rank().over(w3).alias("age_rank"),
    )
)

joined_accounts = (
    with_windows
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.name").alias("customer_name"),
        F.col("p.signup_date"),
        F.col("p.email"),
    )
)

scored = (
    joined_accounts
    .select(
        "id",
        "name",
        "seq_no",
        "age_rank",
        "email",
        F.dense_rank().over(w_grp).alias("age_rank_dense_rank"),
        F.ntile(5).over(w5).alias("seq_no_bucket"),
        F.sum("seq_no").over(w6).alias("running_seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "name",
        "email",
        "age_rank_dense_rank",
        "seq_no_bucket",
        "running_seq_no",
        F.lead("age_rank_dense_rank").over(w7).alias("next_age_rank_dense_rank"),
        F.lag("age_rank_dense_rank").over(w7).alias("prev_age_rank_dense_rank"),
        F.lag("age_rank").over(w7).alias("prev_age_rank"),
        F.lead("age_rank").over(w7).alias("next_age_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)
