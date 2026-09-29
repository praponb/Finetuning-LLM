from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") > F.to_date(F.lit("2023-05-29"))))
    .select(
        "id",
        "customer_id",
        "balance",
        F.date_add("opened_date", 62).alias("opened_date_plus_62d"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.age") > 36)
    .select("s.id", "s.customer_id", "s.balance", F.col("b.city"))
)
