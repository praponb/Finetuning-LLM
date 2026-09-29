from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc())
w2 = Window.orderBy(F.col("balance").desc_nulls_last())
w3 = Window.partitionBy("status", "customer_id")

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-07-21"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("account_type").isin(["CHECKING", "CREDIT"]), "Tier 1").otherwise("Other").alias("account_type_group"),
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

remaining_keys = (
    filtered
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 32)
        .select("status")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("status") == "FROZEN")
    .select(
        "customer_id",
        "status",
        "status_clean",
        F.rank().over(w).alias("balance_rank"),
        F.percent_rank().over(w2).alias("balance_percent_rank"),
        F.min("balance").over(w3).alias("status_min_balance"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.signup_date"), F.col("c.email"), F.col("c.age"))
)

result = (
    joined
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.country").alias("country"), F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.balance_rank")).alias("avg_balance_rank"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
