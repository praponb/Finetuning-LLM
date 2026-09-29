from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("credit_limit_rounded").desc(), "id")
w3 = Window.partitionBy("city").orderBy("running_min_credit_limit_rounded", "id")
w4 = Window.partitionBy("segment").orderBy("running_sum_credit_limit_rounded", "id")

valid = (
    df_customers
    .filter(F.col("name").like("%Dia%"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        F.concat("country", F.lit(", "), "city").alias("country_city"),
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
    )
)

scored = (
    valid
    .filter(F.col("credit_limit_rounded").between(24, 52))
    .select(
        "*",
        F.sum("credit_limit_rounded").over(w_grp).alias("running_sum_credit_limit_rounded"),
        F.min("credit_limit_rounded").over(w_grp).alias("running_min_credit_limit_rounded"),
        F.max("credit_limit_rounded").over(w_grp).alias("running_max_credit_limit_rounded"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.balance") <= 52000), "left_semi")
)

enriched = (
    kept
    .select(
        "id",
        "email",
        "country",
        "city",
        "credit_limit",
        "running_max_credit_limit_rounded",
        "row_seq",
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(3).over(w2).alias("credit_limit_rounded_bucket"),
        F.lag("row_seq", 1).over(w4).alias("prev_row_seq"),
    )
)

result = enriched.select("id", "email", "city", "credit_limit", "row_seq", "prev_row_seq")
