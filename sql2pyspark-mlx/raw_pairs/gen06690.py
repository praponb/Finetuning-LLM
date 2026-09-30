from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_last())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy("credit_limit", "id")
w4 = Window.partitionBy("segment").orderBy(F.col("age_share").asc_nulls_last(), "id")

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-05").cast("date")) & (F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select("*", F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"))
)

with_windows = (
    base
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.cume_dist().over(w).alias("age_cume_dist"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.ntile(4).over(w3).alias("credit_limit_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "country",
        F.col("age_share").alias("AGE_SHARE"),
        F.col("age_cume_dist").alias("AGE_CUME_DIST"),
    )
    .unpivot(
        ids=["id", "country"],
        values=["AGE_SHARE", "AGE_CUME_DIST"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
