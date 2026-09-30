from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.length(F.col("c.email")) < 6) & F.col("c.country").isin(["US", "SG"]))
    .select(
        F.col("c.segment"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.date_trunc("year", F.col("a.opened_date")).alias("opened_date_period"),
    )
    .orderBy("segment")
)
