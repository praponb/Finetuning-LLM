from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.city",
        F.col("c.balance").alias("account_balance"),
        F.col("c.status"),
    )
)

remaining_keys = (
    with_last
    .select("city")
    .intersectAll(
        df_customers
        .filter(~F.col("name").like("D%"))
        .select("city")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (~F.col("x.account_type").isin(["SAVINGS", "CHECKING", "LOAN"])), "left_anti")
)

filtered = (
    kept
    .filter(F.col("status").isNotNull())
    .select(
        "email",
        "country",
        "city",
        "status",
        F.upper("status").alias("status_upper"),
    )
)

result = filtered.select("email", "status_upper")
