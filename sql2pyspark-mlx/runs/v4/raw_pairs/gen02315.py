from pyspark.sql import functions as F

not_flagged = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.balance") >= 72000), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "PRIORITY").when(F.col("segment").rlike("^(Corporate)$"), "CORE").otherwise("EXTENDED").alias("segment_zone"),
    )
)
